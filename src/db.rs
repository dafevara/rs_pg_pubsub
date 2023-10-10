
extern crate dotenv;

use dotenv::dotenv;
use std::env;
use postgres::{Client, NoTls};
use indicatif::ProgressBar;


pub fn connection_desc() -> String {
    dotenv().ok();
    let pg_user = env::var("PG_USER").unwrap();
    let pg_pass = env::var("PG_PASSWORD").unwrap();
    let pg_database = env::var("PG_DATABASE").unwrap();

    let desc = format!("host=localhost user={} password={} dbname={}", pg_user, pg_pass, pg_database);

    return desc.to_string()
}

pub fn create_tables() -> Result<(), postgres::Error>{

    let desc: String = connection_desc();
    let connect = Client::connect(&desc, NoTls);
    let mut pg = match connect {
        Ok(client) => client,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    pg.batch_execute("drop table if exists users cascade;
        create table if not exists users (
            id serial primary key,
            name text not null,
            email text not null,
            balance int not null default 0
        );
        drop table if exists products cascade;
        create table if not exists products (
            id serial primary key,
            name text not null,
            price int not null,
            stock int not null default 0,
            discount int not null default 0
        );

        drop type if exists payment_status cascade;
        create type payment_status as enum (
            'accepted', 'failed', 'pending'
        );

        drop table if exists payments cascade;
        create table if not exists payments (
            id serial primary key,
            product_id int,
            user_id int,
            amount int not null default 0,
            status payment_status default 'pending',
            CONSTRAINT fk_product_id
                FOREIGN KEY(product_id)
                REFERENCES products(id),
            CONSTRAINT fk_user_id
                FOREIGN KEY(user_id)
                REFERENCES users(id)

        );
        drop table if exists payment_tasks cascade;
        create table if not exists payment_tasks (
            id serial primary key,
            payment_id int,
            tries_left int not null default 5,
            error text,
            processing bool not null default false,
            next_try_at timestamp,
            created_at timestamp not null default CURRENT_TIMESTAMP,
            updated_at timestamp not null default CURRENT_TIMESTAMP,
            CONSTRAINT fk_payment_id
                FOREIGN KEY(payment_id)
                REFERENCES payments(id)
        );

        CREATE OR REPLACE FUNCTION insert_into_payment_task()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO payment_tasks (payment_id) VALUES (NEW.id);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        drop trigger if exists process_payment_trigger on payments cascade;
        CREATE TRIGGER process_payment_trigger
        AFTER INSERT
        ON payments
        FOR EACH ROW
        EXECUTE FUNCTION insert_into_payment_task();

    ")
}


pub fn populate_base_data(n: i32) -> Result<(), std::io::Error> {

    use fake::{Dummy, Fake, Faker};
    use fake::faker::name::en::*;
    use fake::faker::internet::en::*;
    use fake::faker::company::en::*;

    #[derive(Debug, Dummy)]
    pub struct FakeUser {
        #[dummy(faker = "Name()")]
        name: String,
        #[dummy(faker = "FreeEmail()")]
        email: String,
        #[dummy(faker = "1000..10000")]
        balance: i32,
    }

    #[derive(Debug, Dummy)]
    pub struct FakeProduct {
        #[dummy(faker = "Buzzword()")]
        name: String,
        #[dummy(faker = "1000..10000")]
        price: i32,
        #[dummy(faker = "0..100")]
        stock: i32,
        #[dummy(faker = "0..50")]
        discount: i32,
    }

    let desc: String = connection_desc();
    let mut pg = match Client::connect(&desc, NoTls) {
        Ok(pg) => pg,
        Err(err_desc) => panic!("{:?}", err_desc)
    };

    println!("Populating users");

    let bar = ProgressBar::new(n as u64);
    for _ in 0..n {
        let u: FakeUser = Faker.fake();
        let query = "insert into users (name, email, balance) values ($1, $2, $3)";
        if let Err(err_insert) = pg.execute(query, &[&u.name, &u.email, &u.balance]) {
            panic!("{:?}", err_insert);
        }
        bar.inc(1)

    }
    bar.finish();

    println!("Populating products");

    let bar = ProgressBar::new(n as u64);

    for _ in 0..(n/10) {
        let p: FakeProduct = Faker.fake();
        let query = "insert into products (name, price, stock, discount) values ($1, $2, $3, $4)";
        if let Err(err_insert) = pg.execute(query, &[&p.name, &p.price, &p.stock, &p.discount]) {
            panic!("{:?}", err_insert);
        }
        bar.inc(1);
    }
    bar.finish();

    Ok(())
}
