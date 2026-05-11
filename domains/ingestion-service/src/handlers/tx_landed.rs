use rpc_core::types::TxLanded;
use sqlx::{Pool, Postgres};
use tracing::{debug};

pub async fn handle(pool: &Pool<Postgres>, tx: TxLanded) -> anyhow::Result<()> {
    // 1. Update tx_results with landing info
    // We compute landing_time_ms by subtracting transactions.submitted_at from our geyser_landed_at
    let result = sqlx::query(
        r#"
        UPDATE tx_results SET
            status = 'landed',
            geyser_landed_at = $1,
            landed_slot = $2,
            landing_time_ms = (
                EXTRACT(EPOCH FROM ($1 - t.submitted_at)) * 1000
            )::INT
        FROM transactions t
        WHERE tx_results.transaction_id = t.id
          AND tx_results.signature = $3
          AND tx_results.status = 'pending'
        "#,
    )

    .bind(tx.geyser_landed_at)
    .bind(tx.landed_slot)
    .bind(&tx.signature)
    .execute(pool)
    .await?;



    if result.rows_affected() == 0 {
        // This can happen if tx.landed arrives before tx.submitted (rare but possible with Kafka)
        // or if it was already updated.
        debug!(sig = %tx.signature, "tx.landed: No pending row updated (maybe arrived early or already processed)");
    } else {
        debug!(sig = %tx.signature, provider = %tx.provider_id, "Ingested tx.landed");
    }

    Ok(())
}
