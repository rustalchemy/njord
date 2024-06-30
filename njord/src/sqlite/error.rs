

use rusqlite::Error as RusqliteError;

/// Represents errors that can occur during SQLite operations.
#[derive(Debug)]
pub enum SqliteError {
    /// Error that occurs during a SELECT operation.
    SelectError(RusqliteError),
    /// Error that occurs during an INSERT operation.
    InsertError(RusqliteError),
    /// Error that occurs during an UPDATE operation.
    UpdateError(RusqliteError),
    /// Error that occurs during a DELETE operation.
    DeleteError(RusqliteError),
}

impl From<RusqliteError> for SqliteError {
    /// Converts a `rusqlite::Error` into a `SqliteError`.
    fn from(error: RusqliteError) -> Self {
        SqliteError::InsertError(error)
    }
}
