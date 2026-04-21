use rpc_core::types::TxTimeout;
use sqlx::{Pool, Postgres};
use tracing::debug;

pub async fn handle(pool: &Pool<Postgres>, tx: TxTimeout) -> anyhow::Result<()> {
    // Update tx_results to 'timeout' status if it's still pending
    let result = sqlx::query(
        r#"
        UPDATE tx_results SET
            status = 'timeout'
        WHERE signature = $1
          AND status = 'pending'
        "#,
    )
    .bind(&tx.signature)
    .execute(pool)
    .await?;



    if result.rows_affected() > 0 {
        debug!(sig = %tx.signature, provider = %tx.provider_id, "Ingested tx.timeout");
    }

    Ok(())
}
