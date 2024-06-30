

use std::path::Path;

use rusqlite::{Connection, Error};

pub mod delete;
pub mod error;
pub mod insert;
pub mod select;
pub mod update;
mod util;

pub use delete::delete;
pub use error::SqliteError;
pub use insert::insert;
pub use select::select;
pub use update::update;

/// Open a database connection.
///
/// This function opens a connection to a SQLite database located at the specified path.
///
/// # Arguments
///
/// * `db_path` - A reference to the path where the SQLite database is located.
///
/// # Returns
///
/// Returns a `Result` containing a `Connection` if the operation was successful, or an `Error` if an error occurred.
///
/// # Errors
///
/// This function can return an error if:
///
/// * The specified database path does not exist.
/// * There are permission issues when trying to access the database.
/// * The database is corrupted or not a valid SQLite database.
pub fn open(db_path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(db_path)?;

    Ok(conn)
}

/// Open an in-memory database connection.
///
/// This function opens a connection to an in-memory SQLite database.
///
/// # Returns
///
/// Returns a `Result` containing a `Connection` if the operation was successful, or an `Error` if an error occurred.
///
/// # Errors
///
/// This function can return an error if:
///
/// * There was an issue creating the in-memory database connection.
pub fn open_in_memory() -> Result<Connection, Error> {
    let conn = Connection::open_in_memory()?;

    Ok(conn)
}
