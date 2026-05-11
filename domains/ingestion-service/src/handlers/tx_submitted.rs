use rpc_core::types::TxSubmitted;
use sqlx::{Pool, Postgres, Row};
use tracing::{debug};


pub async fn handle(pool: &Pool<Postgres>, tx: TxSubmitted) -> anyhow::Result<()> {
    // 1. Insert into transactions table
    // If signature exists, we just skip (it might be a retry or duplicate event)
    let tx_id = sqlx::query(
        r#"
        INSERT INTO transactions (
            signature, provider_id, region_id, fee_tier_id,
            submitted_at, submitted_slot, network_tps, batch_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (signature) DO UPDATE SET signature = EXCLUDED.signature
        RETURNING id
        "#,
    )
    .bind(&tx.signature)
    .bind(&tx.provider_id)
    .bind(&tx.region_id)
    .bind(&tx.fee_tier_id)
    .bind(tx.submitted_at)
    .bind(tx.submitted_slot)
    .bind(tx.network_tps)
    .bind(uuid::Uuid::parse_str(&tx.batch_id).unwrap_or_default())
    .fetch_one(pool)
    .await?
    .get::<i64, _>("id");

    // 2. Initialize tx_results entry
    sqlx::query(
        r#"
        INSERT INTO tx_results (transaction_id, signature, provider_id, status)
        VALUES ($1, $2, $3, 'pending')
        ON CONFLICT (transaction_id) DO NOTHING
        "#,
    )
    .bind(tx_id)
    .bind(&tx.signature)
    .bind(&tx.provider_id)
    .execute(pool)
    .await?;


    debug!(sig = %tx.signature, provider = %tx.provider_id, "Ingested tx.submitted");
    Ok(())
}
