use sqlx::{types::BigDecimal, Executor, Transaction};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(128)
        .connect("postgres://postgres:postgres@127.0.0.1:5432/postgres")
        .await
        .expect("Failed to connect to Postgres");

    // clean up the database
    let _ = clean_up(&pool).await.expect("Failed to clean up database");
    // add some accounts
    for i in 0..100 {
        let address = format!("0x{0:x}", i);
        add_account(&pool, &address, 1000).await
            .expect("Failed to add account");
    }

    let mut futs = JoinSet::new();
    for i in 0..10000 {
        let pool = pool.clone();
        let from = format!("0x{0:x}", 0);
        let to = format!("0x{0:x}", i % 100);
        futs.spawn(async move {
            let Ok(tx) = pool.begin().await else {
                tracing::warn!("Failed to start transaction");
                return;
            };
            let tx_hash = format!("{:x}", i);
            let amount = 3;
            let res = bad_transfer(tx, &tx_hash, &from, &to, amount).await;
            // let res = good_transfer(tx, &tx_hash, &from, &to, amount).await;
            match res {
                Ok(_) => {},
                Err(e) => {
                    tracing::error!("Error: {:?}", e);
                }
            }
        });
    }

    futs.join_all().await;

    let from = format!("0x{0:x}", 0);
    let diff = account_balance_verify(&pool, &from).await.expect("Failed to verify account consistency");

    if diff >= BigDecimal::from(0) {
        tracing::info!("Account consistency verified");
    } else {
        tracing::error!("Account consistency verification failed: {}", diff);
    }
}

async fn clean_up<'a, E>(executor: E) -> sqlx::Result<u64>
where E: Executor<'a, Database = sqlx::Postgres>
{
    sqlx::query!(
        r#"
        truncate transaction, accounts
        "#,
    )
    .execute(executor)
    .await
    .map(|res| res.rows_affected())
}

async fn add_account<'a, E>(executor: E, address: &str, initial: u64) -> sqlx::Result<u64>
where E: Executor<'a, Database = sqlx::Postgres>
{
    sqlx::query!(
        r#"
        INSERT INTO accounts (address, balance)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        "#,
        address,
        initial as i64
    )
    .execute(executor)
    .await
    .map(|res| res.rows_affected())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Insufficient funds account({0})")]
    InsufficientFunds(String),
    #[error("Account not found: {0}")]
    AccountNotFound(String),
    #[error("unknown error: {0}")]
    Other(String),
}

#[allow(unused)]
async fn good_transfer<'a>(mut tx: Transaction<'a, sqlx::Postgres>, tx_hash: &str, from: &str, to: &str, amount: u64) -> Result<u64, Error>
{
    let insert_rows: u64 = sqlx::query!(
        r#"
        INSERT INTO transaction (tx_hash, from_address, to_address, amount)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        "#,
        tx_hash,
        from,
        to,
        amount as i64
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if insert_rows == 0 {
        tracing::info!("Transaction already exists");
        return Ok(0);
    }

    let update_from_rows: u64 = sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = balance - $1, updated_at = now()
        WHERE address = $2 AND balance >= $1
        "#,
        amount as i64,
        from
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if update_from_rows == 0 {
        tracing::info!("Insufficient funds");
        return Err(Error::InsufficientFunds(from.to_string()));
    }

    let update_to_rows: u64 = sqlx::query!(
        r#"
        INSERT INTO accounts (address, balance, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (address) DO UPDATE
        SET balance = accounts.balance + EXCLUDED.balance, updated_at = now()
        "#,
        to,
        amount as i64,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if update_to_rows == 0 {
        tracing::info!("Failed to update recipient account");
        return Err(Error::Other("Failed to update recipient account".to_string()));
    }

    tx.commit().await?;
    Ok(update_from_rows)
}

#[allow(unused)]
async fn bad_transfer<'a>(mut tx: Transaction<'a, sqlx::Postgres>, tx_hash: &str, from: &str, to: &str, amount: u64) -> Result<u64, Error>
{
    // BAD1: No lock on the account balance
    let from_balance = sqlx::query!(
        r#"
        SELECT balance
        FROM accounts
        WHERE address = $1
        "#,
        from
    )
    .fetch_optional(&mut *tx)
    .await?
    .map(|row| row.balance)
    .ok_or(Error::AccountNotFound(from.to_string()))?;

    if from_balance < amount as i64 {
        tracing::info!("Insufficient funds");
        return Err(Error::InsufficientFunds(from.to_string()));
    }

    // BAD1: Because there is no lock on the account balance
    // BAD1: the balance can be updated by another transaction
    let update_from_rows = sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = balance - $1, updated_at = now()
        WHERE address = $2
        "#,
        amount as i64,
        from
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if update_from_rows != 1 {
        tracing::info!("Failed to update sender account");
        return Err(Error::Other("Failed to update sender account".to_string()));
    }

    let update_to_rows = sqlx::query!(
        r#"
        UPDATE accounts
        SET balance = balance + $1, updated_at = now()
        WHERE address = $2
        "#,
        amount as i64,
        to
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if update_to_rows != 1 {
        tracing::info!("Failed to update recipient account");
        return Err(Error::Other("Failed to update recipient account".to_string()));
    }

    let insert_rows = sqlx::query!(
        r#"
        INSERT INTO transaction (tx_hash, from_address, to_address, amount)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        "#,
        tx_hash,
        from,
        to,
        amount as i64
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if insert_rows == 0 {
        tracing::info!("Transaction already exists");
        return Ok(0);
    }
    
    tx.commit().await?;
    Ok(insert_rows)
}

async fn account_balance_verify<'a, E>(executor: E, address: &str) -> Result<BigDecimal, Error>
where E: Executor<'a, Database = sqlx::Postgres>
{
    let balance = sqlx::query!(
        r#"
        SELECT balance
        FROM accounts
        WHERE address = $1
        "#,
        address
    )
    .fetch_one(executor)
    .await?
    .balance;

    Ok(BigDecimal::from(balance))
}

#[allow(unused)]
async fn ledger_consistency_verify<'a, E>(executor: E, initial: u64) -> Result<BigDecimal, Error>
where E: Executor<'a, Database = sqlx::Postgres>
{
    let all = sqlx::query!(
        r#"
        select address,
            balance,
            case when credit
                is null then 0
                else credit
            end as credit,
            case when debit
                is null then 0
                else debit
            end as debit
        from accounts,
            LATERAL (
                select sum(amount) as credit
                from transaction
                where from_address = accounts.address
            ) as credit,
            LATERAL (
                select sum(amount) as debit
                from transaction
                where to_address = accounts.address
            ) as debit;
        "#
    )
    .fetch_all(executor)
    .await?;

    let mut diff = BigDecimal::from(0);
    for row in all {
        let balance = BigDecimal::from(row.balance);
        let credit = row.credit.unwrap_or(BigDecimal::from(0));
        let debit = row.debit.unwrap_or(BigDecimal::from(0));
        if balance != initial + &credit - &debit {
            diff += balance - (initial + credit - debit);
        }
    }

    Ok(diff)
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn test_add_account() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect("postgres://postgres:postgres@127.0.0.1:5432/postgres")
            .await
            .unwrap();

        let address = "0x1234567890abcdef".to_string();
        let rows_affected = add_account(&pool, &address, 1000).await.unwrap();
        assert_eq!(rows_affected, 1);

        let rows_affected = add_account(&pool, &address, 1000).await.unwrap();
        assert_eq!(rows_affected, 0);
    }
}