use actix_web::{get, web, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt as _;
use tokio::time::{interval, Duration};

#[get("/ws")]
async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, actix_web::Error> {
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    // Spawn task to handle incoming messages
    actix_web::rt::spawn(async move {
        // Send a ping every 20 seconds to keep connection alive
        let mut ping_interval = interval(Duration::from_secs(20));
        // Align interval with the next 30s boundary (00 or 30)
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let next_30s = 30 - (now % 30);
        let mut leaderboard_interval = interval(Duration::from_secs(30));
        // Skip the immediate tick
        leaderboard_interval.tick().await;
        
        // Wait until the next 30s mark before starting the loop's interval logic
        tokio::time::sleep(Duration::from_secs(next_30s)).await;

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    let _ = session.ping(b"").await;
                }
                
                _ = leaderboard_interval.tick() => {
                    let _ = session.text("leaderboard_updated").await;
                }

                Some(Ok(msg)) = msg_stream.next() => {
                    match msg {
                        Message::Ping(bytes) => {
                            let _ = session.pong(&bytes).await;
                        }
                        Message::Close(reason) => {
                            let _ = session.close(reason).await;
                            break;
                        }
                        _ => {} // Ignore text/binary
                    }
                }

                else => break, // Connection closed
            }
        }
    });

    Ok(response)
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(ws_handler);
}
