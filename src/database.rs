use anyhow::Context;
use anyhow::Result;
use chrono::prelude::*;
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct LeaderboardEntry {
    pub username: String,
    pub books_read: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct MonthWinner {
    pub username: String,
    pub month: String,
    pub year: String,
    pub books_read: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct YearWinner {
    pub username: String,
    pub year: String,
    pub books_read: i64,
}

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    sqlx::FromRow,
)]
struct Integer64(i64);

impl Integer64 {
    pub fn into_inner(self) -> i64 {
        self.0
    }
}

#[derive(Debug)]
pub struct Database {
    pool: Pool<Sqlite>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    pub async fn try_init() -> Result<Self> {
        let pool = match std::path::Path::new("./bookshelf_database.db").exists() {
            true => {
                SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect("sqlite://./bookshelf_database.db")
                    .await?
            }
            false => {
                let _ = std::fs::File::create("./bookshelf_database.db")?;
                SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect("sqlite://./bookshelf_database.db")
                    .await?
            }
        };
        let mut db = Database { pool };
        db.ensure_tables().await?;
        Ok(db)
    }

    pub async fn ensure_tables(&mut self) -> Result<()> {
        sqlx::query(
            r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY NOT NULL,
            username TEXT KEY UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY NOT NULL,
            title TEXT KEY UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS user_books_read (
            id INTEGER PRIMARY KEY NOT NULL,
            user_id INTEGER REFERENCES users NOT NULL,
            book_id INTEGER REFERENCES books NOT NULL,
            datetime TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
            UNIQUE(user_id, book_id)
        );

        CREATE TABLE IF NOT EXISTS monthly_wins (
            id INTEGER PRIMARY KEY NOT NULL,
            user_id INTEGER REFERENCES users NOT NULL,
            month TEXT NOT NULL,
            year TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS yearly_wins (
            id INTEGER PRIMARY KEY NOT NULL,
            user_id INTEGER REFERENCES users NOT NULL,
            year TEXT NOT NULL
        );
        "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Ensure a user with the given username exists in the database
    /// Returns the database id of the user
    pub async fn ensure_user<S: AsRef<str>>(&mut self, username: S) -> Result<i64> {
        sqlx::query(
            r#"
        INSERT OR IGNORE INTO users (username)
        VALUES ($1);
        "#,
        )
        .bind(username.as_ref())
        .execute(&self.pool)
        .await?;

        let id = sqlx::query_as::<_, Integer64>(
            r#"
            SELECT id
            FROM users
            WHERE username = $1;
            "#,
        )
        .bind(username.as_ref())
        .fetch_one(&self.pool)
        .await?;

        Ok(id.into_inner())
    }

    /// Ensure a book with the given title exists in the database
    /// Returns the database id of the book
    pub async fn ensure_book<S: AsRef<str>>(&mut self, title: S) -> Result<i64> {
        sqlx::query(
            r#"
        INSERT OR IGNORE INTO books (title)
        VALUES ($1);
        "#,
        )
        .bind(title.as_ref())
        .execute(&self.pool)
        .await?;

        let id = sqlx::query_as::<_, Integer64>(
            r#"
            SELECT id
            FROM books
            WHERE title = $1;
            "#,
        )
        .bind(title.as_ref())
        .fetch_one(&self.pool)
        .await?;

        Ok(id.into_inner())
    }

    /// Mark the given user as having read the given book
    /// User and book will be created if they do not yet exist
    /// Returns the number of books the user has read in total
    pub async fn user_read_book<S: AsRef<str>>(&mut self, username: S, title: S) -> Result<i64> {
        let user_id = self.ensure_user(&username).await?;
        let book_id = self.ensure_book(&title).await?;

        sqlx::query(
            r#"
            INSERT INTO user_books_read (user_id, book_id)
            VALUES ($1, $2);
            "#,
        )
        .bind(user_id)
        .bind(book_id)
        .execute(&self.pool)
        .await?;

        let count = sqlx::query_as::<_, Integer64>(
            r#"
            SELECT COUNT(*) FROM user_books_read
            WHERE user_id = $1;
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?
        .into_inner();

        Ok(count)
    }

    pub async fn user_unread_book<S: AsRef<str>>(&mut self, username: S, title: S) -> Result<i64> {
        let user_id = self.ensure_user(&username).await?;
        let book_id = self.ensure_book(&title).await?;

        sqlx::query(
            r#"
            DELETE FROM user_books_read
            WHERE user_id = $1 AND book_id = $2
            "#,
        )
        .bind(user_id)
        .bind(book_id)
        .execute(&self.pool)
        .await?;

        let count = sqlx::query_as::<_, Integer64>(
            r#"
            SELECT COUNT(*) FROM user_books_read
            WHERE user_id = $1;
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?
        .into_inner();

        Ok(count)
    }

    /// Returns the number of books the given user has read
    pub async fn count_books_read<S: AsRef<str>>(&mut self, username: S) -> Result<i64> {
        let user_id = self.ensure_user(&username).await?;
        let count = sqlx::query_as::<_, Integer64>(
            r#"
            SELECT COUNT(*) FROM user_books_read
            WHERE user_id = $1;
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?
        .into_inner();

        Ok(count)
    }

    /// Returns the number of users who have read the given book
    pub async fn count_users_read<S: AsRef<str>>(&mut self, title: S) -> Result<i64> {
        let book_id = self.ensure_book(&title).await?;
        let count = sqlx::query_as::<_, Integer64>(
            r#"
            SELECT COUNT(*) FROM user_books_read
            WHERE book_id = $1;
            "#,
        )
        .bind(book_id)
        .fetch_one(&self.pool)
        .await?
        .into_inner();

        Ok(count)
    }

    /// Returns the leaderboard for this month, sorted descending
    pub async fn monthly_leaderboard(&self) -> Result<Vec<LeaderboardEntry>> {
        let mut leaderboard: Vec<LeaderboardEntry> = sqlx::query_as(
            r#"
            SELECT DISTINCT users.username, (
                SELECT COUNT(*) FROM user_books_read
                WHERE user_id = users.id
            ) AS books_read
            FROM users
            INNER JOIN user_books_read ON users.id = user_books_read.user_id
            WHERE user_books_read.datetime >= date('now', 'start of month');
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        leaderboard.sort_by(|a, b| b.books_read.cmp(&a.books_read));

        Ok(leaderboard)
    }

    /// Returns the leaderboard for this year, sorted descending
    pub async fn yearly_leaderboard(&self) -> Result<Vec<LeaderboardEntry>> {
        let mut leaderboard: Vec<LeaderboardEntry> = sqlx::query_as(
            r#"
            SELECT DISTINCT users.username, (
                SELECT COUNT(*) FROM user_books_read
                WHERE user_id = users.id
            ) AS books_read
            FROM users
            INNER JOIN user_books_read ON users.id = user_books_read.user_id
            WHERE user_books_read.datetime >= date('now', 'start of year');
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        leaderboard.sort_by(|a, b| b.books_read.cmp(&a.books_read));

        Ok(leaderboard)
    }

    pub async fn win_month(&mut self) -> Result<MonthWinner> {
        let board = self.monthly_leaderboard().await?;
        let winner = board.first().context("No leaderboard entries")?;
        let winner_id = self.ensure_user(&winner.username).await?;
        let now = Utc::now();
        let month: Month = Month::try_from(u8::try_from(now.month())?)?.pred();
        let year = if month == Month::December {
            Utc::now().year() - 1
        } else {
            Utc::now().year()
        };

        sqlx::query(
            r#"
            INSERT INTO monthly_wins (user_id, month, year)
            VALUES ($1, $2, $3);
        "#,
        )
        .bind(winner_id)
        .bind(month.name())
        .bind(year)
        .execute(&self.pool)
        .await?;

        Ok(MonthWinner {
            username: winner.username.clone(),
            month: month.name().to_string(),
            year: year.to_string(),
            books_read: winner.books_read,
        })
    }

    pub async fn win_year(&mut self) -> Result<YearWinner> {
        let board = self.yearly_leaderboard().await?;
        let winner = board.first().context("No leaderboard entries")?;
        let winner_id = self.ensure_user(&winner.username).await?;
        let year = chrono::Utc::now().year() - 1;

        sqlx::query(
            r#"
            INSERT INTO yearly_wins (user_id, year)
            VALUES ($1, $2);
        "#,
        )
        .bind(winner_id)
        .bind(year)
        .execute(&self.pool)
        .await?;

        Ok(YearWinner {
            username: winner.username.clone(),
            year: year.to_string(),
            books_read: winner.books_read,
        })
    }
}
