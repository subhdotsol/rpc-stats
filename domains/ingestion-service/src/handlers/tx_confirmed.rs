use rpc_core::types::TxConfirmed;
use sqlx::{Pool, Postgres};
use tracing::debug;

pub async fn handle(pool: &Pool<Postgres>, tx: TxConfirmed) -> anyhow::Result<()> {
    // Update tx_results with RPC confirmation info
    // We compute rpc_confirm_time_ms by subtracting transactions.submitted_at from our rpc_confirmed_at
    let result = sqlx::query(
        r#"
        UPDATE tx_results SET
            rpc_confirmed_at = $1,
            rpc_confirm_time_ms = (
                EXTRACT(EPOCH FROM ($1 - t.submitted_at)) * 1000
            )::INT
        FROM transactions t
        WHERE tx_results.transaction_id = t.id
          AND tx_results.signature = $2
        "#,
    )
    .bind(tx.rpc_confirmed_at)
    .bind(&tx.signature)
    .execute(pool)
    .await?;



    if result.rows_affected() > 0 {
        debug!(sig = %tx.signature, provider = %tx.provider_id, "Ingested tx.confirmed");
    }

    Ok(())
}
