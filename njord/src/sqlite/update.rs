

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

/// Constructs a new UPDATE query builder.
///
/// # Arguments
///
/// * `conn` - A `rusqlite::Connection` to the SQLite database.
/// * `table` - An instance of the table to be updated.
///
/// # Returns
///
/// An `UpdateQueryBuilder` instance.
pub fn update<T: Table + Default>(conn: Connection, table: T) -> UpdateQueryBuilder<T> {
    UpdateQueryBuilder::new(conn, table)
}

/// A builder for constructing UPDATE queries.
pub struct UpdateQueryBuilder<T: Table + Default> {
    conn: Connection,
    table: Option<T>,
    columns: Vec<String>,
    where_condition: Option<Condition>,
    order_by: Option<HashMap<Vec<String>, String>>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<T: Table + Default> UpdateQueryBuilder<T> {
    /// Creates a new `UpdateQueryBuilder` instance.
    ///
    /// # Arguments
    ///
    /// * `conn` - A `rusqlite::Connection` to the SQLite database.
    /// * `table` - An instance of the table to be updated.
    pub fn new(conn: Connection, table: T) -> Self {
        UpdateQueryBuilder {
            conn,
            table: Some(table),
            columns: Vec::new(),
            where_condition: None,
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Sets the columns and values to be updated.
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of strings representing the columns to be updated.
    pub fn set(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
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
    /// * `count` - The maximum number of rows to be updated.
    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count);
        self
    }

    /// Sets the OFFSET clause for the query.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset from which to start updating rows.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Builds and executes the UPDATE query.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the update operation.
    pub fn build(self) -> Result<(), String> {
        let table_name = self
            .table
            .as_ref()
            .map(|t| t.get_name().to_string())
            .unwrap_or("".to_string());

        // Sanitize table name from unwanted quotations or backslashes
        let table_name_str = remove_quotes_and_backslashes(&table_name);

        // Generate SET clause
        let set = if let Some(table) = &self.table {
            let mut set_fields = Vec::new();
            let fields = table.get_column_fields();
            let values = table.get_column_values();

            for column in &self.columns {
                // Check if column exists in the table's fields
                if let Some(index) = fields.iter().position(|c| c == column) {
                    let value = values.get(index).cloned().unwrap_or_default();
                    let formatted_value = if value.is_empty() {
                        "NULL".to_string()
                    } else if value.parse::<f64>().is_ok() {
                        value
                    } else {
                        format!("'{}'", value)
                    };
                    set_fields.push(format!("{} = {}", column, formatted_value));
                } else {
                    // Handle the case when the column doesn't exist in the table
                    eprintln!("Column '{}' does not exist in the table", column);
                }
            }

            set_fields.join(", ")
        } else {
            String::new()
        };

        let where_condition_str = generate_where_condition_str(self.where_condition);
        let order_by_str = generate_order_by_str(&self.order_by);
        let limit_str = generate_limit_str(self.limit);
        let offset_str = generate_offset_str(self.offset);

        // Construct the query based on defined variables above
        let query = format!(
            "UPDATE {} SET {} {} {} {}",
            table_name_str,
            set,
            where_condition_str,
            order_by_str,
            format!("{} {}", limit_str, offset_str),
        );

        info!("{}", query);
        println!("{}", query);

        // Prepare SQL statement
        match self.conn.prepare(query.as_str()) {
            Ok(_) => println!("Success!"),
            Err(_) => eprintln!("Could not execute..."),
        };

        Ok(())
    }
}
