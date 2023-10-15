

extern crate dotenv;

use dotenv::dotenv;
use std::env;
use chrono::{DateTime, Local, Utc, NaiveDateTime};
use postgres::{Client, NoTls};
use postgres::row::Row;
use indicatif::ProgressBar;
use std::{thread, time};

use crate::db;
use crate::model::{PaymentTask, Payment, User, Product};


fn next() -> Option<PaymentTask> {

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
                updated_at < CURRENT_TIMESTAMP - INTERVAL '1 SEC'
            )
            ORDER BY next_try_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, payment_id, tries_left, error, processing, updated_at
    "#;
    let result = match pg.query_one(query, &[]) {
        Ok(result) => Some(PaymentTask::from(result)),
        Err(desc) => None
    };

    result
}

fn success(payment_task: PaymentTask, payment: Payment, product: Product, new_balance: i32) -> () {

    let desc: String = db::connection_desc();
    let mut pg = match Client::connect(&desc, NoTls) {
        Ok(pg) => pg,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    // update user's with new balance
    let mut q = "update users set balance = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&new_balance, &payment.user_id]) {
        panic!("{:?}", err_update);
    }
    // update payment with success state
    let mut q = "update payments set status = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&"accepted", &payment_task.payment_id]) {
        panic!("{:?}", err_update);
    }

    println!("Successful payment. New Balance: {:?}", new_balance);

    // update product with new stock
    let new_stock = (product.stock - 1);
    println!("New Stock: {:?}", new_stock);
    let mut q = "update products set stock = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&new_stock, &product.id]) {
        panic!("{:?}", err_update);
    }

    // remove message from queue
    let query = "delete from payment_tasks where id = $1";
    if let Err(err_delete) = pg.execute(query, &[&payment_task.id]) {
        panic!("{:?}", err_delete);
    }
}

fn failed_by_balance (payment_task: PaymentTask, user: User, payment: Payment, product: Product) -> () {

    let balance: i32 = user.balance;
    let price: i32 = product.price;
    let msg = format!("Unable to pay because price: {} is greater than balance {}", price, balance);

    let desc: String = db::connection_desc();
    let mut pg = match Client::connect(&desc, NoTls) {
        Ok(pg) => pg,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    let mut q = "update payment_tasks set error = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&msg, &payment_task.id]) {
        panic!("{:?}", err_update);
    }

    let mut q = "update payments set status = 'rejected' where id = $1 ";
    if let Err(err_update) = pg.execute(q, &[&payment.id]) {
        panic!("{:?}", err_update);
    }
}

fn failed_by_stock (payment_task: PaymentTask, user: User, payment: Payment) -> () {
    let msg = "Unable to pay because there's no stock";
    let desc: String = db::connection_desc();
    let mut pg = match Client::connect(&desc, NoTls) {
        Ok(pg) => pg,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    let mut q = "update payment_tasks set error = $1 where id = $2";
    if let Err(err_update) = pg.execute(q, &[&msg, &payment_task.id]) {
        panic!("{:?}", err_update);
    }

    let mut q = "update payments set status = 'rejected' where id = $1 ";
    if let Err(err_update) = pg.execute(q, &[&payment.id]) {
        panic!("{:?}", err_update);
    }
}

fn perform(payment_task: PaymentTask) -> () {
    println!("Processing Task: {:?}", payment_task);

    let desc: String = db::connection_desc();
    let mut pg = match Client::connect(&desc, NoTls) {
        Ok(pg) => pg,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    let mut q = "select * from payments where id = $1";
    let payment = match pg.query_one(q, &[&payment_task.payment_id]) {
        Ok(payment) => Payment::from(payment),
        Err(desc) => panic!("No payment was found")
    };
    println!("Payment: {:?}", payment);

    let mut q = "select * from users where id = $1";
    let user = match pg.query_one(q, &[&payment.user_id]) {
        Ok(user) => User::from(user),
        Err(desc) => panic!("no user found")
    };
    println!("for User: {:?}", user);

    let mut q = "select * from products where id = $1";
    let product = match pg.query_one(q, &[&payment.product_id]) {
        Ok(product) => Product::from(product),
        Err(desc) => panic!("no user found")
    };
    println!("{:?}", product);

    let balance: i32 = user.balance;
    let price: i32 = product.price;
    let stock: i32 = product.stock;

    let new_balance: i32 = balance - price;
    let new_stock: i32 = stock - 1;

    if (new_balance < 0) {
        failed_by_balance(payment_task, user, payment, product)
    } else if (new_stock < 0) {

        failed_by_stock(payment_task, user, payment)
    } else {
        success(payment_task, payment, product, new_balance)
    }

}

pub fn attach() -> Result<(), std::io::Error> {
    let ten_millis = time::Duration::from_millis(5);
    loop {
        if let Some(payment_task) = next(){
            perform(payment_task);
        } else {
            println!("waiting ...");
            thread::sleep(ten_millis)
        }
    }

    Ok(())
}
