

extern crate dotenv;

use dotenv::dotenv;
use std::env;
use postgres::{Client, NoTls};
use postgres::row::Row;
use indicatif::ProgressBar;

use crate::db;

#[derive(Debug)]
pub struct PaymentTaskRow {
    pub id: i32,
    pub payment_id: i32,
    pub tries_left: i32,
    pub error: String,
    pub processing: bool,
    pub next_try_at: String,
    pub created_at: String,
    pub updated_at: String
}


impl From<Row> for PaymentTaskRow {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            payment_id: row.get("payment_id"),
            tries_left: row.get("tries_left"),
            error: row.get("error"),
            processing: row.get("processing"),
            next_try_at: row.get("next_try_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at")
        }
    }
}

fn next() -> Result<PaymentTaskRow, postgres::Error> {

    let desc: String = db::connection_desc();
    let mut pg = match Client::connect(&desc, NoTls) {
        Ok(pg) => pg,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    let query = r#"
        UPDATE payment_tasks SET
            processing = true,
            tries_left = tries_left - 1,
            error = NULL,
            next_try_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = (
            SELECT id
            FROM payment_tasks
            WHERE tries_left > 0
            AND (
                next_try_at IS NULL OR
                next_try_at < CURRENT_TIMESTAMP
            )
            AND (
                processing = false OR
                updated_at < CURRENT_TIMESTAMP - INTERVAL '5 SEC'
            )
            ORDER BY next_try_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING *
    "#;
    let row = pg.query_one(query, &[])?;

    Ok(PaymentTaskRow::from(row))

}

pub fn attach() -> Result<(), std::io::Error> {
    loop {
        let payment_task = next();
        println!("{:?}", payment_task)
    }

    Ok(())
}
