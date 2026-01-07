use anyhow::Context as _;
use anyhow::Error;
use anyhow::Result;
use bookshelf_bot::database::LeaderboardEntry;
use chrono::TimeDelta;
use poise::serenity_prelude as serenity;

use ::serenity::all::ChannelId;
use ::serenity::all::CreateEmbed;
use ::serenity::all::CreateMessage;
use ::serenity::all::EditMessage;
use ::serenity::all::GetMessages;
use ::serenity::all::GuildId;
use ::serenity::all::Message;
use ::serenity::async_trait;
use serenity::prelude::*;

use bookshelf_bot::database::Database;
use chrono::prelude::*;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;

static DB: OnceLock<Mutex<Database>> = OnceLock::new();
static NEXT_MONTHLY_WIN: OnceLock<Mutex<DateTime<Utc>>> = OnceLock::new();
static NEXT_YEARLY_WIN: OnceLock<Mutex<DateTime<Utc>>> = OnceLock::new();
static CHAN_WRITER: OnceLock<tokio::sync::mpsc::Sender<()>> = OnceLock::new();

// Automatically keep monthly and yearly leaderboards
// Simple command to mark a book as completed
// Interface for old people

#[derive(Debug, Clone, Eq, PartialEq)]
enum WinState {
    NotTime,
    MonthEnd {
        username: String,
        month: String,
        year: String,
        books_read: i64,
    },
    YearMonthEnd {
        year_winner: String,
        month_winner: String,
        month: String,
        year: String,
        month_books: i64,
        year_books: i64,
    },
}

struct Data {}
type Context<'a> = poise::Context<'a, Data, Error>;

#[poise::command(slash_command)]
async fn count(ctx: Context<'_>) -> Result<()> {
    tracing::trace!("Counting books");
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = ctx.author();
    tracing::trace!("Getting count");
    let count = db.count_books_read(&username.name).await?;
    ctx.send(
        poise::CreateReply::default()
            .content(format!("You have read {} books.", count))
    )
    .await?;
    Ok(())
}

#[poise::command(slash_command)]
async fn unread(ctx: Context<'_>, #[description = "Book Title"] title: String) -> Result<()> {
    tracing::trace!("Attempting to remove read entry for book");
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = ctx.author();
    tracing::trace!("Updating database");
    let Ok(count) = db.user_unread_book(&username.name, &title).await else {
        tracing::trace!("User has not read book");
        ctx.send(
            poise::CreateReply::default()
                .content(format!("You haven't read*{}*!", &title))
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
                &title, count
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
async fn read(ctx: Context<'_>, #[description = "Book Title"] title: String) -> Result<()> {
    tracing::trace!("Adding book {} to user {}", title, ctx.author());
    tracing::trace!("Acquiring mutex");
    let mut db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
    let username = ctx.author();
    tracing::trace!("Updating database");
    let Ok(count) = db.user_read_book(&username.name, &title).await else {
        tracing::trace!("User already read book");
        ctx.send(
            poise::CreateReply::default()
                .content(format!("You have already read *{}*!", &title))
                .ephemeral(true),
        )
        .await?;
        return Ok(());
    };
    tracing::trace!("Responding to user");
    ctx.send(
        poise::CreateReply::default()
            .content(format!(
                "You have marked *{}* as read! You have read {} books total",
                &title, count
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
async fn month(ctx: Context<'_>) -> Result<()> {
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
async fn year(ctx: Context<'_>) -> Result<()> {
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

fn make_leaderboard(entries: &[LeaderboardEntry]) -> String {
    let mut out = String::new();
    for (i, entry) in entries.iter().enumerate() {
        if i >= 10 {
            break;
        }

        let plural = if entry.books_read > 1 { "s" } else { "" };
        let prefix = if i == 0 {
            ":first_place:"
        } else if i == 1 {
            ":second_place:"
        } else if i == 2 {
            ":third_place:"
        } else {
            &format!("{i}.")
        };
        out.push_str(&format!(
            "{} **{}** {} — book{} read\n",
            prefix, entry.username, entry.books_read, plural
        ));
    }
    out
}

async fn check_time_and_assign_wins(ctx: Arc<serenity::prelude::Context>) -> Result<WinState> {
    tracing::trace!("Acquiring mutex locks");
    let win_state = {
        let month_win_time = NEXT_MONTHLY_WIN
            .get()
            .context("Monthly Win Time not set")?
            .lock()
            .await;
        let year_win_time = NEXT_YEARLY_WIN
            .get()
            .context("Yearly Win Time not set")?
            .lock()
            .await;
        let now = Utc::now();
        let mut win_state = WinState::NotTime;

        let time_to_month: chrono::TimeDelta = month_win_time.signed_duration_since(now);
        let time_to_year: chrono::TimeDelta = year_win_time.signed_duration_since(now);
        tracing::trace!("Checking if time to win");
        tracing::trace!(
            "time-month: {}, time-year: {}",
            time_to_month.as_seconds_f64(),
            time_to_year.as_seconds_f64()
        );

        if time_to_month < chrono::TimeDelta::zero() {
            // Choose a monthly winner!
            tracing::trace!("Update DB monthly");
            let month_winner = DB
                .get()
                .context("Failed to acquire Database Mutex")?
                .lock()
                .await
                .win_month()
                .await?;
            win_state = WinState::MonthEnd {
                username: month_winner.username,
                month: month_winner.month,
                year: month_winner.year,
                books_read: month_winner.books_read,
            };
        }

        if time_to_year < chrono::TimeDelta::zero() {
            // Choose a yearly winner!
            tracing::trace!("Update DB yearly");
            let year_winner = DB
                .get()
                .context("Failed to acquire Database Mutex")?
                .lock()
                .await
                .win_year()
                .await?;

            let WinState::MonthEnd {
                username,
                month,
                year,
                books_read,
            } = win_state
            else {
                unreachable!();
            };

            win_state = WinState::YearMonthEnd {
                year_winner: year_winner.username,
                month_winner: username,
                month,
                year,
                month_books: books_read,
                year_books: year_winner.books_read,
            };
        }

        win_state
    };
    tracing::trace!("Reinitialize win times");
    init_next_win_times().await?;
    tracing::trace!("Done");
    Ok(win_state)
}

struct WinChecker {
    is_loop_running: AtomicBool,
}

#[async_trait]
impl EventHandler for WinChecker {
    async fn cache_ready(&self, ctx: serenity::prelude::Context, guilds: Vec<GuildId>) {
        tracing::trace!("Initialize next win times");
        init_next_win_times().await.unwrap();
        let ctx = Arc::new(ctx);
        tracing::trace!("Setting up live leaderboard channel");
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(100);
        CHAN_WRITER.get_or_init(|| tx);

        if !self.is_loop_running.load(Ordering::Relaxed) {
            let ctx = Arc::clone(&ctx);
            tracing::trace!("Starting other threads");
            tracing::trace!("Starting live leaderboard thread");
            update_live_thread(Arc::clone(&ctx), rx).await.unwrap();
            tracing::trace!("Starting win checker thread");
            check_win_thread(ctx);
            self.is_loop_running.swap(true, Ordering::Relaxed);
        }
    }
}

async fn update_live_thread(
    ctx: Arc<serenity::prelude::Context>,
    mut rx: tokio::sync::mpsc::Receiver<()>,
) -> Result<()> {
    // Search for a channel called 'live-leaderboard'
    // If not exists, return
    // If does, clear all message history in that channel, send the new leaderboards, update them
    // whenever a message is waiting on the channel
    let msg = {
        let cache = ctx.cache().context("No cache found in context!").unwrap();
        let guilds = cache.guilds();
        let guild_id = guilds.first().context("No guilds in cache!").unwrap();
        let guild = guild_id.to_partial_guild(Arc::clone(&ctx)).await?;
        let channels = guild.channels(Arc::clone(&ctx)).await?;
        let mut msg: Option<Message> = None;
        for (_, channel) in channels {
            if channel.name == "live-leaderboard" {
                tracing::trace!("live leaderboard channel found");
                tracing::trace!("delete 100 messages");
                let get_messages = GetMessages::new().limit(100);
                let messages = channel.messages(Arc::clone(&ctx), get_messages).await?;
                if !messages.is_empty() {
                    channel.delete_messages(Arc::clone(&ctx), messages).await?;
                }
                tracing::trace!("send new leaderboards");
                let db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
                let monthly = db.monthly_leaderboard().await?;
                let yearly = db.yearly_leaderboard().await?;
                let yearly = make_leaderboard(&yearly);
                let monthly = make_leaderboard(&monthly);

                let year = chrono::Utc::now().format("%Y").to_string();
                let month_year = chrono::Utc::now().format("%B %Y").to_string();

                msg = Some(channel
                    .send_message(
                        Arc::clone(&ctx),
                        CreateMessage::new()
                            .embeds(vec![
                                CreateEmbed::new().title(format!("Live {} Yearly Leaderboard", year)).description(yearly),
                                CreateEmbed::new().title(format!("Live {} Monthly Leaderboard", month_year)).description(monthly)
                            ]),
                    )
                    .await?);
                break;
            }
        }
        msg
    };

    if msg.is_none() {
        tracing::trace!("No live leaderboard channel found");
        return Ok(());
    }

    tokio::spawn(async move {
        let Some(mut msg) = msg else {
            unreachable!();
        };
        let ctx = ctx;
        loop {
            if (rx.recv().await).is_some() {
                let db = DB.get().context("Failed to acquire DB Mutex").unwrap().lock().await;
                let monthly = db.monthly_leaderboard().await.unwrap();
                let yearly = db.yearly_leaderboard().await.unwrap();
                let yearly = make_leaderboard(&yearly);
                let monthly = make_leaderboard(&monthly);

                let year = chrono::Utc::now().format("%Y").to_string();
                let month_year = chrono::Utc::now().format("%B %Y").to_string();

                msg.edit(Arc::clone(&ctx), EditMessage::new().embeds(vec![
                    CreateEmbed::new().title(format!("Live {} Yearly Leaderboard", year)).description(yearly),
                    CreateEmbed::new().title(format!("Live {} Monthly Leaderboard", month_year)).description(monthly)
                ])).await.unwrap();
            }
        }
    });

    Ok(())
}

fn check_win_thread(ctx: Arc<serenity::prelude::Context>) {
    tokio::spawn(async move {
        loop {
            tracing::trace!("Entered win check loop");
            let win_state = check_time_and_assign_wins(Arc::clone(&ctx)).await.unwrap();
            match win_state {
                WinState::NotTime => {}
                WinState::MonthEnd {
                    username,
                    month,
                    year,
                    books_read,
                } => {
                    tracing::trace!("Time to win month");
                    let channel = {
                        let cache = ctx.cache().context("No cache found in context!").unwrap();
                        let guilds = cache.guilds();
                        let guild_id = guilds.first().context("No guilds in cache!").unwrap();
                        let guild = cache
                            .guild(guild_id)
                            .context("No guild found with given id!")
                            .unwrap();
                        guild
                            .default_channel_guaranteed()
                            .context("No default channel")
                            .unwrap()
                            .clone()
                    };
                    let msg = CreateMessage::new().content("@everyone").embed(
                        CreateEmbed::new()
                            .title(format!(":trophy: {} {} Winner", month, year))
                            .description(format!(
                                "{} read the most books this month at {}!",
                                username, books_read
                            )),
                    );
                    channel.send_message(&ctx, msg).await.unwrap();
                }
                WinState::YearMonthEnd {
                    year_winner,
                    month_winner,
                    month,
                    year,
                    month_books,
                    year_books,
                } => {
                    let channel = {
                        let cache = ctx.cache().context("No cache found in context!").unwrap();
                        let guilds = cache.guilds();
                        let guild_id = guilds.first().context("No guilds in cache!").unwrap();
                        let guild = cache
                            .guild(guild_id)
                            .context("No guild found with given id!")
                            .unwrap();
                        guild
                            .default_channel_guaranteed()
                            .context("No default channel")
                            .unwrap()
                            .clone()
                    };
                    let msg = CreateMessage::new().content("@everyone").embeds(vec![
                        CreateEmbed::new()
                            .title(format!(":trophy: {} {}  Winner", month, year))
                            .description(format!(
                                "{} read the most books this month at {}!",
                                month_winner, month_books
                            )),
                        CreateEmbed::new()
                            .title(format!(":trophy {} Overall Winner", year))
                            .description(format!(
                                "{} read the most books this year at {}! Happy New Year!",
                                year_winner, year_books
                            )),
                    ]);
                    channel.send_message(&ctx, msg).await.unwrap();
                }
            }
            tracing::trace!("End win check loop");
            tokio::time::sleep(std::time::Duration::from_mins(10)).await;
        }
    });
}

async fn init_next_win_times() -> Result<()> {
    let now = Utc::now();
    let month: Month = Month::try_from(u8::try_from(now.month())?)?.succ();
    let year: i32 = now.year() + 1;
    let monthly_year = if month == Month::January {
        year
    } else {
        year - 1
    };

    let mut monthly_lock = NEXT_MONTHLY_WIN
        .get_or_init(|| Mutex::new(Utc::now()))
        .lock()
        .await;
    let mut yearly_lock = NEXT_YEARLY_WIN
        .get_or_init(|| Mutex::new(Utc::now()))
        .lock()
        .await;

    *monthly_lock = Utc
        .with_ymd_and_hms(monthly_year, month.number_from_month(), 1, 0, 0, 0)
        .single()
        .context("Failed to Utcify DateTime for monthly schedule")?
        - TimeDelta::minutes(30);

    *yearly_lock = Utc
        .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .context("Failed to Utcify DateTime for yearly schedule")?
        - TimeDelta::minutes(30);

    tracing::info!(
        "Setting next monthly win time to {}",
        monthly_lock.format("%d %B %Y at %H:%M:%S")
    );
    tracing::info!(
        "Setting next yearly win time to {}",
        yearly_lock.format("%d %B %Y at %H:%M:%S")
    );

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let e_filter = tracing_subscriber::EnvFilter::new("info,bookshelf_bot=trace");
    dotenvy::dotenv()?;
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt().with_env_filter(e_filter).init();
    #[cfg(not(debug_assertions))]
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let db = Database::try_init().await?;
    DB.get_or_init(|| Mutex::new(db));

    let token = std::env::var("DISCORD_TOKEN")?;
    let intents = GatewayIntents::non_privileged() | GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;

    let framework = poise::Framework::builder()
        .setup(|ctx, ready, framework| {
            Box::pin(async move {
                println!("Logged in as {}", ready.user.name);
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                Ok(Data {})
            })
        })
        .options(poise::FrameworkOptions {
            commands: vec![read(), unread(), count(), month(), year()],
            ..Default::default()
        })
        .build();
    let mut client = Client::builder(&token, intents)
        .event_handler(WinChecker {
            is_loop_running: AtomicBool::new(false),
        })
        .framework(framework)
        .await?;

    if let Err(e) = client.start().await {
        println!("Client error: {e:?}");
    }

    Ok(())
}
