

use std::collections::HashMap;

use crate::{
    condition::Condition,
    sqlite::util::{
        generate_limit_str, generate_offset_str, generate_order_by_str,
        generate_where_condition_str, remove_quotes_and_backslashes,
    },
};

use rusqlite::{Connection, Result};

use log::info;

use crate::table::Table;

/// Constructs a new DELETE query builder.
///
/// # Arguments
///
/// * `conn` - A `rusqlite::Connection` to the SQLite database.
///
/// # Returns
///
/// A `DeleteQueryBuilder` instance.
pub fn delete<T: Table + Default>(conn: Connection) -> DeleteQueryBuilder<T> {
    DeleteQueryBuilder::new(conn)
}

/// A builder for constructing DELETE queries.
pub struct DeleteQueryBuilder<T: Table + Default> {
    conn: Connection,
    table: Option<T>,
    where_condition: Option<Condition>,
    order_by: Option<HashMap<Vec<String>, String>>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<T: Table + Default> DeleteQueryBuilder<T> {
    /// Creates a new `DeleteQueryBuilder` instance.
    ///
    /// # Arguments
    ///
    /// * `conn` - A `rusqlite::Connection` to the SQLite database.
    pub fn new(conn: Connection) -> Self {
        DeleteQueryBuilder {
            conn,
            table: None,
            where_condition: None,
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Sets the table from which to delete data.
    ///
    /// # Arguments
    ///
    /// * `table` - An instance of the table from which to delete data.
    pub fn from(mut self, table: T) -> Self {
        self.table = Some(table);
        self
    }

    /// Sets the WHERE clause condition.
    ///
    /// # Arguments
    ///
    /// * `condition` - The condition to be applied in the WHERE clause.
    pub fn where_clause(mut self, condition: Condition) -> Self {
        self.where_condition = Some(condition);
        self
    }

    /// Sets the ORDER BY clause columns and order direction.
    ///
    /// # Arguments
    ///
    /// * `col_and_order` - A hashmap representing the columns and their order directions.
    pub fn order_by(mut self, col_and_order: HashMap<Vec<String>, String>) -> Self {
        self.order_by = Some(col_and_order);
        self
    }

    /// Sets the LIMIT clause for the query.
    ///
    /// # Arguments
    ///
    /// * `count` - The maximum number of rows to be deleted.
    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count);
        self
    }

    /// Sets the OFFSET clause for the query.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset from which to start deleting rows.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Builds and executes the DELETE query.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the deletion operation.
    pub fn build(mut self) -> Result<(), String> {
        let table_name = self
            .table
            .as_ref()
            .map(|t| t.get_name().to_string())
            .unwrap_or("".to_string());

        // Sanitize table name from unwanted quotations or backslashes
        let table_name_str = remove_quotes_and_backslashes(&table_name);
        let where_condition_str = generate_where_condition_str(self.where_condition);
        let order_by_str = generate_order_by_str(&self.order_by);
        let limit_str = generate_limit_str(self.limit);
        let offset_str = generate_offset_str(self.offset);

        // Construct the query based on defined variables above
        let query = format!(
            "DELETE FROM {} {} {} {}",
            table_name_str,
            where_condition_str,
            order_by_str,
            format!("{} {}", limit_str, offset_str),
        );

        info!("{}", query);
        println!("{}", query);

        // Execute SQL
        let _ = match self.conn.transaction() {
            Ok(tx) => tx.execute(&query.to_string(), []),
            Err(_) => todo!(),
        };

        Ok(())
    }
}
