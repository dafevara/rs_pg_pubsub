A simple Rust Implementation of Pub/Sub Message Queue with Postgres

It was designed to simulate a basic payment transaction processing.

The `payment_tasks` represents the message queue. As you can imaging, I'm taking adventage of postgres as _register_ in a typical pub/sub architecture.

ps: you don't need kafka or redis, etc in most of the cases but, let's keep that as a secret between you and me.

**Usage**

It has a cli interface to execute 4 commands:
1. init: setup the db with basic tables like user, payment, etc.
2. populate: populate basic tables with fake data to simulate a queue
3. publish: populate messages into the queue meaning, records in the _payment_tasks_ table.
4. subscribe: listen the queue for messages to process.

More details? just run:

```
cargo run help
```

Please note this is just a naive code. It's not intended nor designed to run in production.

Feedback welcome.
