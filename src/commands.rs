use crate::CHAN_WRITER;
use crate::DB;
use crate::INTERACTION_WAKER;
use crate::database::BookRead;
use crate::database::Since;
use crate::make_leaderboard;
use anyhow::Context as _;
use anyhow::Result;
use nucleo::Config;
use nucleo::Matcher;
use nucleo::pattern::CaseMatching;
use nucleo::pattern::Normalization;
use nucleo::pattern::Pattern;
use poise::Modal;
use poise::serenity_prelude as serenity;

use ::serenity::all::ButtonStyle;
use ::serenity::all::CreateActionRow;
use ::serenity::all::CreateButton;
use chrono::prelude::*;
use std::collections::BTreeMap;
use uuid::Uuid;

#[poise::command(slash_command)]
pub async fn count(
    ctx: crate::Context<'_>,
    #[description = "Username"] username: Option<String>,
) -> Result<()> {
    tracing::info!("{} used `{}`", ctx.author().name, ctx.command().name);
    tracing::trace!("Counting books");
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = username.unwrap_or(ctx.author().name.clone());
    tracing::trace!("Getting count");
    let count = db.count_books_read(&username).await?;
    ctx.send(poise::CreateReply::default().content(format!("You have read {} books.", count)))
        .await?;
    Ok(())
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
struct YearMonth {
    year: i32,
    month: u32,
}

#[poise::command(slash_command)]
pub async fn history(
    ctx: crate::Context<'_>,
    #[description = "Time Period"] time_period: Option<Since>,
    #[description = "Username"] username: Option<String>,
) -> Result<()> {
    tracing::info!("{} used `{}`", ctx.author().name, ctx.invocation_string());
    tracing::trace!("Listing all read books for {}", ctx.author().name);
    tracing::trace!("Acquiring Mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = username.unwrap_or(ctx.author().name.clone());
    tracing::trace!("Getting list");
    let list = db
        .books_read_by(&username, time_period.unwrap_or(Since::Forever))
        .await?;
    tracing::trace!("{:?}", list);
    let mut content = String::new();
    let mut books_map: BTreeMap<YearMonth, Vec<String>> = BTreeMap::new();
    tracing::trace!("Creating month map");
    for entry in list {
        tracing::trace!("--Parsing datetime");
        tracing::trace!("{}", &entry.datetime);
        let datetime = NaiveDateTime::parse_from_str(&entry.datetime, "%Y-%m-%d %H:%M:%S")?;
        let datetime = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
        let year_month = YearMonth {
            year: datetime.year(),
            month: datetime.month(),
        };
        tracing::trace!("--Inserting entry");
        books_map
            .entry(year_month)
            .and_modify(|v| v.push(entry.title.clone()))
            .or_insert(vec![entry.title.clone()]);
    }

    tracing::trace!("Creating output string");
    for (year_month, titles) in books_map {
        let month: chrono::Month = chrono::Month::try_from(u8::try_from(year_month.month)?)?;
        let year_month_str = format!("{} {}", month.name(), year_month.year);
        content += format!("## {}\n", year_month_str).as_str();
        for title in titles {
            content += format!("- {}\n", title).as_str();
        }
    }

    ctx.send(
        poise::CreateReply::default().embed(
            serenity::CreateEmbed::new()
                .title(format!("Books Read By {}:", ctx.author().name))
                .description(content),
        ),
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command)]
pub async fn unread(
    ctx: crate::Context<'_>,
    #[description = "Book Title"] title: String,
) -> Result<()> {
    tracing::info!("{} used `{:?}`", ctx.author().name, ctx.invocation_string());
    tracing::trace!("Attempting to remove read entry for book");
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = ctx.author();
    tracing::trace!("Updating database");
    let Ok(count) = db.user_unread_book(&username.name, &title).await else {
        tracing::trace!("User has not read book");
        ctx.send(
            poise::CreateReply::default()
                .content(format!("You haven't read*{}*!", title))
                .ephemeral(true),
        )
        .await?;
        return Ok(());
    };

    tracing::trace!("Responding to user");
    ctx.send(
        poise::CreateReply::default()
            .content(format!(
                "You have unmarked *{}* as read! You have read {} books total",
                title, count
            ))
            .ephemeral(true),
    )
    .await?;
    tracing::trace!("Alerting live leaderboard to update");
    CHAN_WRITER.get().unwrap().send(()).await?;
    tracing::trace!("Done");

    tracing::trace!("Cleanup unread books");
    db.cleanup_unread_books().await?;

    Ok(())
}

#[poise::command(slash_command)]
pub async fn read(
    ctx: crate::Context<'_>,
    #[description = "Book Title"] title: String,
) -> Result<()> {
    let mut rx = INTERACTION_WAKER.get().unwrap().subscribe();
    tracing::info!("{} used `{:?}`", ctx.author().name, ctx.invocation_string());
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = ctx.author();

    if let Ok(books) = db.all_books().await {
        let books: Vec<&str> = books.iter().map(|b| b.0.as_str()).collect();
        let mut matcher = Matcher::new(Config::DEFAULT);
        let matches: Vec<&str> =
            Pattern::parse(title.as_str(), CaseMatching::Ignore, Normalization::Smart)
                .match_list(books, &mut matcher)
                .iter()
                .map(|m| m.0)
                .collect();
        if !matches.is_empty() {
            let mut title_list = String::new();
            let mut first = true;
            for m in matches {
                title_list.push_str(&format!("- *{}* ", m.trim_end()));
                if !first {
                    title_list.push('\n');
                }
                first = false;
            }
            let continue_uuid = Uuid::new_v4();
            let cancel_uuid = Uuid::new_v4();
            let reply = poise::CreateReply::default()
                .content(format!(
                    "Book title was similar to the following books. Do you still want to proceed?\n{}",
                    title_list
                ))
                .ephemeral(true)
                .components(vec![CreateActionRow::Buttons(vec![
                    CreateButton::new(format!("{continue_uuid}")).label(format!("Yes, enter the book \"{}\"", &title)),
                    CreateButton::new(format!("{cancel_uuid}")).label("No, I want to try again").style(ButtonStyle::Danger),
                ])]);
            ctx.send(reply).await?;
            loop {
                let response = rx.recv().await.unwrap();
                if response.id == continue_uuid.to_string() {
                    _ = ctx
                        .send(
                            poise::CreateReply::default()
                                .content("Entering the book anyway")
                                .ephemeral(true),
                        )
                        .await;
                    // just proceed :)
                    break;
                } else if response.id == cancel_uuid.to_string() {
                    _ = ctx
                        .send(
                            poise::CreateReply::default()
                                .content("Cancelling")
                                .ephemeral(true),
                        )
                        .await;
                    return Ok(());
                }
            }
        }
    } else {
        tracing::error!("Failed to acquire book history to check for typos, skipping fuzzy match");
    }

    tracing::trace!("Adding book {} to user {}", title, ctx.author());
    tracing::trace!("Updating database");
    let Ok(count) = db.user_read_book(&username.name, &title).await else {
        tracing::trace!("User already read book");
        if let Err(e) = ctx
            .send(
                poise::CreateReply::default()
                    .content(format!("You have already read *{}*!", title))
                    .ephemeral(true),
            )
            .await
        {
            tracing::error!("An error occurred while trying to reply: \n{}", e);
            tracing::error!(
                "Possibly due to trying to reply to a command which has timed out. The command otherwise executed successfully"
            );
            _ = ctx.defer_ephemeral().await;
            return Ok(());
        };
        return Ok(());
    };
    tracing::trace!("Responding to user");
    ctx.send(
        poise::CreateReply::default()
            .content(format!(
                "You have marked *{}* as read! You have read {} books total",
                title, count
            ))
            .ephemeral(true),
    )
    .await?;
    tracing::trace!("Alerting live leaderboard to update");
    CHAN_WRITER.get().unwrap().send(()).await?;
    tracing::trace!("Done");

    Ok(())
}

#[poise::command(slash_command)]
pub async fn month(ctx: crate::Context<'_>) -> Result<()> {
    tracing::info!("{} used `{}`", ctx.author().name, ctx.command().name);
    let db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let monthly = db.monthly_leaderboard().await?;
    let month_year = chrono::Utc::now().format("%B %Y").to_string();
    let board = make_leaderboard(&monthly);

    ctx.send(
        poise::CreateReply::default().embed(
            serenity::CreateEmbed::new()
                .title(format!(":trophy: {} Monthly Leaderboard", month_year))
                .description(board),
        ),
    )
    .await?;

    Ok(())
}

#[poise::command(slash_command)]
pub async fn year(ctx: crate::Context<'_>) -> Result<()> {
    tracing::info!("{} used `{}`", ctx.author().name, ctx.command().name);
    let db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let yearly = db.yearly_leaderboard().await?;
    let year = chrono::Utc::now().format("%Y").to_string();
    let board = make_leaderboard(&yearly);
    ctx.send(
        poise::CreateReply::default().embed(
            serenity::CreateEmbed::new()
                .title(format!(":trophy: {} Yearly Leaderboard", year))
                .description(board),
        ),
    )
    .await?;

    Ok(())
}

#[poise::command(slash_command)]
pub async fn edit(
    ctx: crate::Context<'_>,
    #[description = "Book Title"] title: String,
) -> Result<()> {
    match ctx {
        poise::Context::Application(app) => {
            let mut db = DB.get().unwrap().lock().await;
            let username = ctx.author().name.as_str();
            let mut book = db.book_read_by(username, &title).await?;
            book.datetime.push('Z');
            let datetime: chrono::DateTime<Utc> = book.datetime.parse()?;
            let month: chrono::Month = (datetime.month() as u8).try_into()?;

            let res = poise::execute_modal(
                app,
                Some(EditModal {
                    title: book.title.clone(),
                    month: month.name().to_string(),
                    year: datetime.year().to_string(),
                }),
                Some(std::time::Duration::from_secs(600)),
            )
            .await?;

            if res.is_none() {
                return Ok(());
            }

            let res = res.unwrap();
            let new_year = res.year.parse::<i32>()?;
            let Ok(new_month): Result<chrono::Month, _> = res.month.parse() else {
                ctx.send(
                    poise::CreateReply::default()
                        .content("Failed to parse month. Did you make a typo?")
                        .ephemeral(true),
                )
                .await?;
                return Ok(());
            };
            let new_datetime = NaiveDateTime::new(
                NaiveDate::from_ymd_opt(new_year, new_month.number_from_month(), 1)
                    .context("Failed to parse NaiveDate")?,
                NaiveTime::from_hms_opt(23, 1, 1).unwrap(),
            );
            let new = BookRead {
                username: book.username.clone(),
                title: res.title,
                datetime: new_datetime.to_string(),
            };
            db.update_book_read(book, new).await?;
            ctx.send(
                poise::CreateReply::default()
                    .content("Updated entry successfully!")
                    .ephemeral(true),
            )
            .await?;
            db.cleanup_unread_books().await?;

            Ok(())
        }
        poise::Context::Prefix(_) => {
            ctx.reply("Must use inside an application context").await?;
            Ok(())
        }
    }
}

#[derive(Modal)]
pub struct EditModal {
    title: String,
    month: String,
    year: String,
}
