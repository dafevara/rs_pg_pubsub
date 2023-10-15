use chrono::{DateTime, Local, Utc, NaiveDateTime};
use postgres::row::Row;

#[derive(Debug, PartialEq)]
pub struct PaymentTask {
    pub id: i32,
    pub payment_id: i32,
    pub tries_left: i32,
    pub error: String,
    pub processing: bool,
    pub updated_at: NaiveDateTime
}

#[derive(Debug, PartialEq)]
pub struct Product {
    pub id: i32,
    pub name: String,
    pub price: i32,
    pub stock: i32,
    pub discount: i32,
}

#[derive(Debug, PartialEq)]
pub struct Payment {
    pub id: i32,
    pub product_id: i32,
    pub user_id: i32,
    pub amount: i32,
    pub status: String
}

#[derive(Debug, PartialEq)]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
    pub balance: i32,
}

impl From<Row> for PaymentTask {
    fn from(row: Row) -> Self {
        let error = match row.get("error") {
            Some(error) => error,
            None => ""
        };

        let _updated_at = row.get::<&str,NaiveDateTime>("updated_at");

        Self {
            id: row.get("id"),
            payment_id: row.get("payment_id"),
            tries_left: row.get("tries_left"),
            error: error.to_string(),
            processing: row.get("processing"),
            updated_at: _updated_at
        }
    }
}

impl From<Row> for Product {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            name: row.get("name"),
            price: row.get("price"),
            stock: row.get("stock"),
            discount: row.get("discount"),
        }
    }
}

impl From<Row> for Payment {
    fn from(row: Row) -> Self {

        let _status: &str = row.get("status");
        Self {
            id: row.get("id"),
            product_id: row.get("product_id"),
            user_id: row.get("user_id"),
            amount: row.get("amount"),
            status: _status.to_string(),
        }
    }
}

impl From<Row> for User {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            name: row.get("name"),
            email: row.get("email"),
            balance: row.get("balance"),
        }
    }
}
