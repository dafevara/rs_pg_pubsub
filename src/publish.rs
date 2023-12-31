
extern crate dotenv;

use dotenv::dotenv;
use std::env;
use postgres::{Client, NoTls};
use tokio_postgres::{NoTls as TNoTls, Error as PgError};
use indicatif::ProgressBar;

use crate::db;

pub async fn payments(n: i32) -> Result<(), PgError> {

    use fake::{Dummy, Fake, Faker};

    #[derive(Debug, Dummy)]
    pub struct FakePayment {
        #[dummy(faker = "1..100")]
        product_id: i32,
        #[dummy(faker = "1..100")]
        user_id: i32,
        #[dummy(faker = "10..10000")]
        amount: i32
    }

    let desc: String = db::connection_desc();
    let pg = db::get_client().await?;

    println!("Populating users");

    let bar = ProgressBar::new(n as u64);
    for _ in 0..n {
        let u: FakePayment = Faker.fake();
        let query = "insert into payments (product_id, user_id, amount) values ($1, $2, $3)";
        if let Err(err_insert) = pg.execute(query, &[&u.product_id, &u.user_id, &u.amount]).await {
            panic!("{:?}", err_insert);
        }
        bar.inc(1)

    }
    bar.finish();

    Ok(())
}
