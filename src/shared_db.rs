use hyper::{client::HttpConnector, Body, Client, Method, Request};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use tokio::sync::Mutex;

use crate::{deserialize, to_bytes};

#[derive(Deserialize, Serialize, Clone)]
pub enum EntryModif {
    Delete(String),
    Update((String, String)),
}

#[derive(Deserialize, Serialize)]
pub struct FetchResult {
    pub head: Head,
    pub entries: Vec<EntryModif>,
    pub diff: Vec<EntryModif>,
}

#[derive(Default)]
pub struct ModifsCache(VecDeque<(Head, Vec<EntryModif>)>);
pub type Head = u32;

impl ModifsCache {
    /// Append new `EntryModif` batches into database,
    /// remove oldest values if buffer exceed [crate::CACHE_BUFFER_SIZE]
    pub fn append(&mut self, modifs: Vec<EntryModif>) {
        // increases monotonically (can be changed with any other logic)
        let new_head: Head = self.head() + 1 % u32::MAX;
        self.0.push_front((new_head, modifs));
        if self.0.len() > crate::CACHE_BUFFER_SIZE {
            self.0.pop_back();
        }
    }

    /// Get the current head of the database
    pub fn head(&self) -> u32 {
        match self.0.back() {
            Some((h, _)) => *h,
            None => 0,
        }
    }

    /// Get a list of EntryModif between the give `head` and the current
    /// head of the SharedDB.
    pub fn diff(&self, head: u32) -> Vec<EntryModif> {
        let mut ret = vec![];
        for (h, ms) in &self.0 {
            if head == *h {
                break;
            } else {
                ret.append(&mut ms.clone());
            }
        }
        ret
    }
}

#[derive(Default)]
pub struct DB {
    /// database, BTreeMap for index ordering
    data: BTreeMap<String, String>,
    /// Remember latest modifications for poll-bootstraper
    cache: ModifsCache,
}

#[derive(Clone, Default)]
pub struct SharedDB(Arc<Mutex<DB>>);

impl SharedDB {
    pub async fn append(&self, modifs: Vec<EntryModif>) {
        let mut guard = self.0.lock().await;
        for m in &modifs {
            match m {
                EntryModif::Delete(key) => guard.data.remove(key),
                EntryModif::Update((key, val)) => guard.data.insert(key.clone(), val.clone()),
            };
        }
        guard.cache.append(modifs);
    }

    /// Head and size of the db
    pub async fn info(&self) -> (u32, usize) {
        let guard = self.0.lock().await;
        (guard.cache.head(), guard.data.len())
    }

    pub async fn fetch(&self, begin: usize, end: usize, head: u32) -> FetchResult {
        let guard = self.0.lock().await;
        let entries = take_chunk(
            &guard.data,
            begin,
            std::cmp::min(crate::MAX_CHUNK_SIZE, end - begin),
        );
        FetchResult {
            head: guard.cache.head(),
            entries,
            diff: guard.cache.diff(head),
        }
    }

    pub async fn dump(&self) {
        for (key, value) in self.0.lock().await.data.iter() {
            println!("{key} - {value}");
        }
    }
}

fn take_chunk(data: &BTreeMap<String, String>, from: usize, size: usize) -> Vec<EntryModif> {
    data.iter()
        .skip(from)
        .take(size)
        .map(|(k, v)| EntryModif::Update((k.clone(), v.clone())))
        .collect()
}

/// Make a /info request through the `client` to the `target`
/// Return a tuple (head, size) or panic if request fail
pub async fn info_request(client: &Client<HttpConnector>, target: &String) -> (u32, usize) {
    let res = client
        .get(format!("http://{}/info", target).parse().unwrap())
        .await
        .unwrap();
    deserialize(&to_bytes(res.into_body()).await)
}

pub async fn fetch_request(
    client: &Client<HttpConnector>,
    target: &String,
    begin: usize,
    end: usize,
    head: u32,
) -> FetchResult {
    let req = Request::builder()
        .method(Method::GET)
        .uri(&format!("http://{target}/fetch"))
        .body(Body::from(
            serde_json::to_string(&(begin, end, head)).unwrap(),
        ))
        .unwrap();
    let res = client.request(req).await.unwrap();
    deserialize(&to_bytes(res.into_body()).await)
}
