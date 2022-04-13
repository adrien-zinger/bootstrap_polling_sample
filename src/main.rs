use hyper::{
    body::Bytes,
    service::{make_service_fn, service_fn},
    Method, StatusCode,
};
use hyper::{Body, Request, Response, Server};
use serde::Deserialize;
use shared_db::{fetch_request, info_request, EntryModif, SharedDB};
use std::net::SocketAddr;
use std::{convert::Infallible, time::Duration};
mod shared_db;

pub const BOOTSTRAP_FETCH_PERIOD: Duration = Duration::from_secs(1);
pub const MAX_CHUNK_SIZE: usize = 20;
pub const CACHE_BUFFER_SIZE: usize = 1000;

async fn services_impl(req: Request<Body>, db: SharedDB) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/insert") => {
            let modifs = deserialize::<Vec<EntryModif>>(&to_bytes(req.into_body()).await);
            db.append(modifs).await;
            Ok(Response::new(Body::default()))
        }
        (&Method::GET, "/info") => Ok(Response::new(Body::from(
            serde_json::to_string(&db.info().await).unwrap(),
        ))),
        (&Method::GET, "/fetch") => {
            let (begin, end, head) =
                deserialize::<(usize, usize, u32)>(&to_bytes(req.into_body()).await);
            Ok(Response::new(Body::from(
                serde_json::to_string(&db.fetch(begin, end, head).await).unwrap(),
            )))
        }
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

fn parse_input() -> (String, Option<String>) {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 {
        (args[1].clone(), None)
    } else if args.len() == 3 {
        (args[1].clone(), Some(args[2].clone()))
    } else {
        println!("error usage:\ncargo run -- {{port}} {{optional bootstrap port}}");
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    let (port, bootstrap_port) = parse_input();
    let addr = SocketAddr::from(([127, 0, 0, 1], port.parse::<u16>().unwrap()));
    let shared_database = SharedDB::default();
    let db = shared_database.clone();
    let make_svc = make_service_fn(move |_| {
        let db = db.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                services_impl(req, db.clone())
            }))
        }
    });
    println!("Start bootstrapable server on addr {}", addr);
    println!("Example of posts insertion body (/insert route):");
    println!(
        "{}",
        serde_json::to_string(&EntryModif::Update((
            "key".to_string(),
            "value".to_string()
        )))
        .unwrap()
    );
    let server = Server::bind(&addr).serve(make_svc);
    let graceful = server.with_graceful_shutdown(shutdown_signal());
    if let Some(p) = bootstrap_port {
        spawn_fetch_loop(shared_database.clone(), format!("127.0.0.1:{p}"));
    }
    if let Err(e) = graceful.await {
        eprintln!("server error: {}", e);
    }
    println!("Shudown... dump entries:\n");
    shared_database.dump().await;
}

fn spawn_fetch_loop(db: SharedDB, target: String) {
    tokio::spawn(async move {
        let client = hyper::client::Client::new();
        let info = info_request(&client, &target).await;
        let mut index = 0;
        let end = info.1;
        let mut head = info.0;
        // todo, dump a progression status
        while index < end {
            let mut res = fetch_request(
                &client,
                &target,
                index,
                std::cmp::min(index + MAX_CHUNK_SIZE, end),
                head,
            )
            .await;
            head = res.head;
            index += MAX_CHUNK_SIZE;
            res.entries.append(&mut res.diff);
            db.append(res.entries).await;
            // you can check here, if for a while, we have no updates from the
            // remote. And exit the loop
            let s = tokio::time::sleep(BOOTSTRAP_FETCH_PERIOD);
            tokio::pin!(s);
            tokio::select! {
                _ = s => continue,
                _ = tokio::signal::ctrl_c() => return,
            };
        }
    });
}

async fn to_bytes(body: Body) -> Bytes {
    hyper::body::to_bytes(body).await.unwrap()
}

fn deserialize<'a, T: Deserialize<'a>>(body_bytes: &'a Bytes) -> T {
    serde_json::from_slice(body_bytes).unwrap()
}
