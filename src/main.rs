use anyhow::Result;
use bookshelf_bot::DB;
use bookshelf_bot::schedule::WinChecker;
use poise::serenity_prelude as serenity;

use serenity::prelude::*;

use bookshelf_bot::database::Database;
use bookshelf_bot::commands::*;
use std::sync::atomic::AtomicBool;
use tokio::sync::Mutex;
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

fn setup_logging() -> Result<()> {
    #[cfg(debug_assertions)]
    let e_filter = tracing_subscriber::EnvFilter::new("info,bookshelf_bot=trace");
    #[cfg(not(debug_assertions))]
    let e_filter = tracing_subscriber::EnvFilter::new("warn,bookshelf_bot=info");

    let stderr_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_writer(std::io::stderr)
        .with_filter(e_filter.clone());

    let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("bookshelf_bot")
        .filename_suffix("log")
        .build("./logs")?;

    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_writer(file_appender)
        .with_filter(e_filter);

    tracing_subscriber::Registry::default()
        .with(stderr_layer)
        .with(file_layer)
        .try_init()?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;
    setup_logging()?;
    {
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            tracing::error!("A panic occurred, a thread may have been lost");
            default_panic(info);
        }));
    }

    let db = Database::try_init().await?;
    DB.get_or_init(|| Mutex::new(db));

    let token = std::env::var("DISCORD_TOKEN")?;
    let intents = GatewayIntents::non_privileged()
        | GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;

    let framework = poise::Framework::builder()
        .setup(|ctx, ready, framework| {
            Box::pin(async move {
                println!("Logged in as {}", ready.user.name);
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                Ok(bookshelf_bot::Data {})
            })
        })
        .options(poise::FrameworkOptions {
            commands: vec![read(), unread(), count(), month(), year(), history(), edit()],
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
