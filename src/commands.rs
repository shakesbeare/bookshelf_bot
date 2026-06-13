use crate::CHAN_WRITER;
use crate::DB;
use crate::database::Since;
use crate::make_leaderboard;
use anyhow::Context as _;
use anyhow::Result;
use poise::serenity_prelude as serenity;

use chrono::prelude::*;
use std::collections::BTreeMap;

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

    Ok(())
}

#[poise::command(slash_command)]
pub async fn read(
    ctx: crate::Context<'_>,
    #[description = "Book Title"] title: String,
) -> Result<()> {
    tracing::info!("{} used `{:?}`", ctx.author().name, ctx.invocation_string());
    tracing::trace!("Adding book {} to user {}", title, ctx.author());
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = ctx.author();
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
