pub mod commands;
pub mod database;
pub mod schedule;

use anyhow::Error;
use chrono::DateTime;
use chrono::Utc;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

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

pub trait TitleCase {
    /// Allocates a new string which is the title-cased version of the input string according to the
    /// APA style guide
    fn title_case(&self) -> String;
}

impl<S: AsRef<str>> TitleCase for S {
    fn title_case(&self) -> String {
        let mut words: Vec<&str> = Vec::new();
        let input = self.as_ref();
        let mut start = 0;
        let mut i = 0;
        for c in input.chars() {
            if c.is_whitespace() || !c.is_alphabetic() {
                words.push(&input[start..i]);
                start = i;
            }
            i += 1;
        }
        words.push(&input[start..i]);

        let mut out = String::new();
        let mut first = true;
        let mut follows_colon = false;
        for word in words {
            let word = word.trim();
            if !first && word.chars().all(|b| b.is_alphabetic()) {
                out.push(' ');
            }
            if first || follows_colon || !word.is_minor() {
                let mut chars = word.chars();
                let Some(hd) = chars.next() else {
                    continue;
                };
                let hd = hd.to_uppercase();
                let tail: String = chars.collect();
                out.push_str(&format!("{}{}", hd, tail));
            } else {
                out.push_str(&word.to_lowercase());
            }

            first = false;
            follows_colon = word == ":";
        }

        out
    }
}

pub trait IsMinor {
    fn is_minor(&self) -> bool;
}

impl<S: AsRef<str>> IsMinor for S {
    fn is_minor(&self) -> bool {
        let s = self.as_ref();
        matches!(
            s,
            "and"
                | "but"
                | "or"
                | "for"
                | "nor"
                | "the"
                | "a"
                | "an"
                | "in"
                | "on"
                | "at"
                | "by"
                | "off"
                | "per"
                | "up"
                | "via"
                | "if"
                | "so"
                | "yet"
        )
    }
}

mod tests {
    use super::*;

    #[test]
    fn title_case() {
        let title = "the great in the escape: a great on the sea in blabbity off a dock";
        let expected = "The Great in the Escape: A Great on the Sea in Blabbity off a Dock";
        let actual = title.title_case();
        assert_eq!(actual, expected);
    }
}
