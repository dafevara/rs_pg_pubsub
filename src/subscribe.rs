

extern crate dotenv;

use dotenv::dotenv;
use std::env;
use chrono::{DateTime, Local, Utc, NaiveDateTime};
use postgres::{Client, NoTls, Error};
use postgres::row::Row;
use indicatif::ProgressBar;
use std::{thread, time};
use tokio::task;
use tokio_postgres::{NoTls as TNoTls, Error as PgError};
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::db;
use crate::model::{PaymentTask, Payment, User, Product};


async fn next() -> Result<Option<PaymentTask>, PgError> {

    let desc: String = db::connection_desc();
    let pg = db::get_client().await?;

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
                updated_at < CURRENT_TIMESTAMP - INTERVAL '1 SEC'
            )
            ORDER BY next_try_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, payment_id, tries_left, error, processing, updated_at
    "#;

    let result = match pg.query_one(query, &[]).await {
        Ok(result) => Some(PaymentTask::from(result)),
        Err(desc) => None
    };

    Ok(result)
}

async fn success(payment_task: PaymentTask, payment: Payment, product: Product, new_balance: i32) -> Result<(), PgError> {

    let desc: String = db::connection_desc();
    let pg = db::get_client().await?;

    // update user's with new balance
    // let mut q = "update users set balance = $1 where id = $2";
    let mut q = "update users set balance = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&new_balance, &payment.user_id]).await {
        panic!("{:?}", err_update);
    }
    // update payment with success state
    q = "update payments set status = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&"accepted", &payment_task.payment_id]).await {
        panic!("{:?}", err_update);
    }

    println!("Successful payment. New Balance: {:?}", new_balance);

    // update product with new stock
    let new_stock = (product.stock - 1);
    println!("New Stock: {:?}", new_stock);
    let mut q = "update products set stock = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&new_stock, &product.id]).await {
        panic!("{:?}", err_update);
    }

    // remove message from queue
    let query = "delete from payment_tasks where id = $1";
    if let Err(err_delete) = pg.execute(query, &[&payment_task.id]).await {
        panic!("{:?}", err_delete);
    }

    Ok(())
}

async fn failed_by_balance (payment_task: PaymentTask, user: User, payment: Payment, product: Product) -> Result<(), PgError> {

    let balance: i32 = user.balance;
    let price: i32 = product.price;
    let msg = format!("Unable to pay because price: {} is greater than balance {}", price, balance);

    println!("Failed: {:?}", msg);

    let desc: String = db::connection_desc();
    let pg = db::get_client().await?;

    let mut q = "update payment_tasks set error = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&msg, &payment_task.id]).await {
        panic!("{:?}", err_update);
    }

    q = "update payments set status = 'rejected' where id = $1 ";
    if let Err(err_update) = pg.execute(q, &[&payment.id]).await {
        panic!("{:?}", err_update);
    }

    Ok(())
}

async fn failed_by_stock (payment_task: PaymentTask, user: User, payment: Payment) -> Result<(), PgError> {
    let msg = "Unable to pay because there's no stock";
    println!("Failed: {:?}", msg);
    let desc: String = db::connection_desc();
    let pg = db::get_client().await?;

    let mut q = "update payment_tasks set error = $1, tries_left = 0 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&msg, &payment_task.id]).await {
        panic!("{:?}", err_update);
    }

    let mut q = "update payments set status = 'rejected' where id = $1 ";
    if let Err(err_update) = pg.execute(q, &[&payment.id]).await {
        panic!("{:?}", err_update);
    }

    Ok(())
}

async fn perform(payment_task: PaymentTask) -> Result<(), PgError>{
    println!("Processing Task: {:?}", payment_task.id);

    let desc: String = db::connection_desc();
    let pg = db::get_client().await?;

    let mut q = "select * from payments where id = $1";
    let payment_row = pg.query_one(q, &[&payment_task.payment_id]).await?;
    let payment = Payment::from(payment_row);
    println!("Payment: {:?}", payment.id);

    let mut q = "select * from users where id = $1";
    let user_row = pg.query_one(q, &[&payment.user_id]).await?;
    let user = User::from(user_row);
    println!("for User: {:?}", user.id);

    let mut q = "select * from products where id = $1";
    let product_row = pg.query_one(q, &[&payment.product_id]).await?;
    let product = Product::from(product_row);
    println!("Product {:?}", product.id);

    let balance: i32 = user.balance;
    let price: i32 = product.price;
    let stock: i32 = product.stock;

    let new_balance: i32 = balance - price;
    let new_stock: i32 = stock - 1;

    if (new_balance < 0) {
        failed_by_balance(payment_task, user, payment, product).await?
    } else if (new_stock < 0) {
        failed_by_stock(payment_task, user, payment).await?
    } else {
        success(payment_task, payment, product, new_balance).await?
    }

    Ok(())
}

pub async fn attach(concurrency_limit: i32) -> Result<(), Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(concurrency_limit as usize)); // Limiting to 10 concurrent tasks

    loop {
        if let Some(payment_task) = next().await? {
            let permit = semaphore.clone().acquire_owned().await?;
            task::spawn(async move {
                perform(payment_task).await;
                drop(permit);
            });
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await; // Non-blocking sleep
        }
    }

    Ok(())
}
