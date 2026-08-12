pub mod commands;
pub mod database;
pub mod schedule;

use anyhow::Error;
use chrono::DateTime;
use chrono::Utc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use std::sync::OnceLock;
use tokio::sync::Mutex;

use crate::database::Database;
use crate::database::LeaderboardEntry;

pub static DB: OnceLock<Mutex<Database>> = OnceLock::new();
pub static NEXT_MONTHLY_WIN: OnceLock<Mutex<DateTime<Utc>>> = OnceLock::new();
pub static NEXT_YEARLY_WIN: OnceLock<Mutex<DateTime<Utc>>> = OnceLock::new();
pub static CHAN_WRITER: OnceLock<mpsc::Sender<()>> = OnceLock::new();
pub static INTERACTION_WAKER: OnceLock<broadcast::Sender<InteractionWaker>> = OnceLock::new();
pub static INTERACTION_WAITER: OnceLock<broadcast::Receiver<InteractionWaker>> = OnceLock::new();

pub struct Data {}
type Context<'a> = poise::Context<'a, Data, Error>;

#[derive(Debug, Clone)]
pub struct InteractionWaker {
    id: String,
}

// Automatically keep monthly and yearly leaderboards
// Simple command to mark a book as completed
// Interface for old people

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WinState {
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

pub fn make_leaderboard(entries: &[LeaderboardEntry]) -> String {
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
            &format!("{}.", i + 1)
        };
        out.push_str(&format!(
            "{} **{}** — {} book{} read\n",
            prefix, entry.username, entry.books_read, plural
        ));
    }
    out
}
