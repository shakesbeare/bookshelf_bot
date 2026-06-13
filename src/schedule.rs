use anyhow::Context as _;
use anyhow::Result;
use chrono::TimeDelta;
use poise::serenity_prelude as serenity;

use ::serenity::all::CreateEmbed;
use ::serenity::all::CreateMessage;
use ::serenity::all::EditMessage;
use ::serenity::all::GetMessages;
use ::serenity::all::GuildId;
use ::serenity::all::Message;
use ::serenity::async_trait;
use serenity::prelude::*;

use chrono::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;

use crate::CHAN_WRITER;
use crate::DB;
use crate::NEXT_MONTHLY_WIN;
use crate::NEXT_YEARLY_WIN;
use crate::WinState;
use crate::make_leaderboard;

#[cfg(debug_assertions)]
static HAS_ANNOUNCED: std::sync::OnceLock<Mutex<bool>> = std::sync::OnceLock::new();

pub struct WinChecker {
    pub is_loop_running: AtomicBool,
}

#[async_trait]
impl EventHandler for WinChecker {
    async fn cache_ready(&self, ctx: serenity::prelude::Context, _guilds: Vec<GuildId>) {
        let ctx = Arc::new(ctx);
        tracing::trace!("Setting up live leaderboard channel");
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(100);
        CHAN_WRITER.get_or_init(|| tx);

        if !self.is_loop_running.load(Ordering::Relaxed) {
            let ctx = Arc::clone(&ctx);
            tracing::trace!("Starting other threads");
            tracing::trace!("Starting live leaderboard thread");
            if let Err(e) = update_live_thread(Arc::clone(&ctx), rx).await {
                tracing::error!("Error while trying to start leaderboard thread: {}", e);
            }
            tracing::trace!("Starting win checker thread");
            check_win_thread(ctx);
            self.is_loop_running.swap(true, Ordering::Relaxed);
        }
    }
}

async fn update_live_thread(
    ctx: Arc<serenity::prelude::Context>,
    rx: tokio::sync::mpsc::Receiver<()>,
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
                if !messages.is_empty()
                    && let Err(e) = channel.delete_messages(Arc::clone(&ctx), messages).await
                {
                    tracing::error!("Failed to delete messages: {}", e);
                    tracing::warn!("Message history may not be deleted, continuing anyway");
                }
                tracing::trace!("send new leaderboards");
                let db = DB.get().context("Failed to acquire DB Mutex")?.lock().await;
                let monthly = db.monthly_leaderboard().await?;
                let yearly = db.yearly_leaderboard().await?;
                let yearly = make_leaderboard(&yearly);
                let monthly = make_leaderboard(&monthly);
                let now: DateTime<Utc> = chrono::Utc::now() - std::time::Duration::from_hours(16);

                let year = now.format("%Y").to_string();
                let month_year = now.format("%B %Y").to_string();

                msg = Some(
                    channel
                        .send_message(
                            Arc::clone(&ctx),
                            CreateMessage::new().embeds(vec![
                                CreateEmbed::new()
                                    .title(format!("Live {} Yearly Leaderboard", year))
                                    .description(yearly),
                                CreateEmbed::new()
                                    .title(format!("Live {} Monthly Leaderboard", month_year))
                                    .description(monthly),
                            ]),
                        )
                        .await?,
                );
                break;
            }
        }
        msg
    };

    if msg.is_none() {
        tracing::trace!("No live leaderboard channel found");
        return Ok(());
    }

    tokio::spawn(live_leaderboard_inner(ctx, msg, rx));

    Ok(())
}

#[deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
async fn live_leaderboard_inner(
    ctx: Arc<serenity::prelude::Context>,
    msg: Option<Message>,
    mut rx: tokio::sync::mpsc::Receiver<()>,
) {
    let Some(mut msg) = msg else {
        unreachable!();
    };
    loop {
        if (rx.recv().await).is_some() {
            let db = match DB.get() {
                None => {
                    tracing::error!(
                        "Failed to acquire DB Mutex. Leaderboard update will not occur this iteration."
                    );
                    continue;
                }
                Some(v) => v.lock().await,
            };

            let monthly = match db.monthly_leaderboard().await {
                Err(e) => {
                    tracing::error!("An error occurred while fetching the monthly leaderboard: ");
                    tracing::error!("{}", e);
                    continue;
                }
                Ok(v) => v,
            };
            let yearly = match db.yearly_leaderboard().await {
                Err(e) => {
                    tracing::error!("An error occurred while fetching the yearly leaderboard: ");
                    tracing::error!("{}", e);
                    continue;
                }
                Ok(v) => v,
            };

            let yearly = make_leaderboard(&yearly);
            let monthly = make_leaderboard(&monthly);

            let year = chrono::Utc::now().format("%Y").to_string();
            let month_year = chrono::Utc::now().format("%B %Y").to_string();

            if msg
                .edit(
                    Arc::clone(&ctx),
                    EditMessage::new().embeds(vec![
                        CreateEmbed::new()
                            .title(format!("Live {} Yearly Leaderboard", year))
                            .description(yearly),
                        CreateEmbed::new()
                            .title(format!("Live {} Monthly Leaderboard", month_year))
                            .description(monthly),
                    ]),
                )
                .await
                .is_err()
            {
                tracing::error!("An error occurred while editing the live leaderboard message");
            }
        }
    }
}

#[deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
fn check_win_thread(ctx: Arc<serenity::prelude::Context>) {
    tokio::spawn(async move {
        loop {
            tracing::trace!("Entered win check loop");
            let win_state = match check_time_and_assign_wins(Arc::clone(&ctx)).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("An error occurred while checking if it was time to win.");
                    tracing::error!("{}", e);
                    continue;
                }
            };

            let channel = {
                let Some(cache) = ctx.cache() else {
                    tracing::error!("No cache found in context, skipping");
                    continue;
                };
                let guilds = cache.guilds();
                let Some(guild_id) = guilds.first() else {
                    tracing::error!("No guilds found in context, skipping");
                    continue;
                };
                let Some(guild) = cache.guild(guild_id) else {
                    tracing::error!("No guild found for given id, skipping");
                    continue;
                };

                match guild.default_channel_guaranteed() {
                    Some(v) => v.clone(),
                    None => {
                        tracing::error!("No default channel found");
                        continue;
                    }
                }
            };

            match win_state {
                WinState::NotTime => {}
                WinState::MonthEnd {
                    username,
                    month,
                    year,
                    books_read,
                } => {
                    tracing::trace!("Time to win month");
                    tracing::info!("Sending monthly win message");
                    let msg = CreateMessage::new().content("@everyone").embed(
                        CreateEmbed::new()
                            .title(format!(":trophy: {} {} Winner", month, year))
                            .description(format!(
                                "{} read the most books this month at {}!",
                                username, books_read
                            )),
                    );
                    if let Err(e) = channel.send_message(&ctx, msg).await {
                        tracing::error!("An error occurred while sending the message: \n{}", e);
                        tracing::error!("Skipping");
                        continue;
                    }
                }
                WinState::YearMonthEnd {
                    year_winner,
                    month_winner,
                    month,
                    year,
                    month_books,
                    year_books,
                } => {
                    tracing::info!("Sending yearly win message");
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
                    if let Err(e) = channel.send_message(&ctx, msg).await {
                        tracing::error!("An error occurred while sending the message: \n{}", e);
                        tracing::error!("Skipping");
                        continue;
                    }
                }
            }
            tracing::trace!("Sleeping until next win time");
            let next_win_time: DateTime<Utc> = *NEXT_MONTHLY_WIN
                .get_or_init(|| Mutex::new(Utc::now()))
                .lock()
                .await;
            let now = Utc::now();
            let Ok(time_until) = (next_win_time - now).to_std() else {
                tracing::error!(
                    "Failed to create a std::time::Duration out of the chrono::TimeDelta"
                );
                tracing::error!("Defaulting to waiting 24 hours");
                tokio::time::sleep(std::time::Duration::from_hours(24)).await;
                continue;
            };
            tokio::time::sleep(time_until).await;
        }
    });
}

async fn check_time_and_assign_wins(_ctx: Arc<serenity::prelude::Context>) -> Result<WinState> {
    let mut times_need_assignment = true;
    tracing::trace!("Acquiring mutex locks");
    let win_state = {
        let mut month_win_lock = NEXT_MONTHLY_WIN.get();

        if month_win_lock.is_none() {
            tracing::warn!(
                "Win times have not been set yet. Is this the first time since the bot launched?"
            );
            tracing::trace!("Initializing win times");
            init_next_win_times().await?;
            tracing::trace!("...Done");
            month_win_lock = NEXT_MONTHLY_WIN.get();
            times_need_assignment = false;
        }

        let month_win_time = month_win_lock
            .context("Monthly Win Time not set. This should be impossible")?
            .lock()
            .await;

        let year_win_time = NEXT_YEARLY_WIN
            .get()
            .context("Yearly Win Time not set. This should be impossible")?
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

    if times_need_assignment {
        tracing::trace!("Reinitialize win times");
        init_next_win_times().await?;
        tracing::trace!("...Done");
    }

    Ok(win_state)
}

async fn init_next_win_times() -> Result<()> {
    let now = Utc::now();
    let mut month: Month = Month::try_from(u8::try_from(now.month())?)?.succ();
    let mut year: i32 = now.year() + 1;
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

    // End of day GMT is 8 hours before pacific time
    // Win should occur at 8 am pacific time on first of month
    // First of month GMT begins 8 hours before midnight pacific, therefore it occurs at 4pm
    //      pacific
    // 8 am is 16 hours after 4pm the previous day
    // Therefore, win should occur 16 hours *after* GMT turns over to the next month.
    // Wins occur at:
    //     8 am pacific
    //     9 am mountain
    let time_delta = TimeDelta::hours(16);

    *monthly_lock = Utc
        .with_ymd_and_hms(monthly_year, month.number_from_month(), 1, 0, 0, 0)
        .single()
        .context("Failed to Utcify DateTime for monthly schedule")?
        + time_delta;

    *yearly_lock = Utc
        .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .context("Failed to Utcify DateTime for yearly schedule")?
        + time_delta;

    tracing::info!(
        "Setting next monthly win time to {}",
        monthly_lock.format("%d %B %Y at %H:%M:%S")
    );
    tracing::info!(
        "Setting next yearly win time to {}",
        yearly_lock.format("%d %B %Y at %H:%M:%S")
    );

    #[cfg(debug_assertions)]
    {
        let mut has = HAS_ANNOUNCED.get_or_init(|| false.into()).lock().await;
        if !(*has) {
            *monthly_lock = Utc::now() + std::time::Duration::from_secs(5);
            *yearly_lock = Utc::now() + std::time::Duration::from_secs(15);
            *has = true;
        }
    }

    Ok(())
}
