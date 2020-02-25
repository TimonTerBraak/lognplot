use serde::{Deserialize, Serialize};
use rocksdb::{DB};
use std::collections::HashSet;

trait Node {
    fn key(&self) -> u64;
}

trait Series {
    fn key(&self) -> u64;
    fn name(&self) -> str;
}

#[derive(Debug, Serialize, Deserialize)]
struct InternalNode {
    key:  u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct LeafNode {
    key:  u64,
}

#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
struct MWayTree {
    key:  u64,
    name: String,
}

#[derive(Debug)]
struct Catalog {
    db: RocksDB,
    series: HashSet<MWayTree>,
}

const CATALOG_INDEX_KEY: u64 = 0;

impl Catalog {

    fn open(mut db: RocksDB) -> Self {
        let series: HashSet<MWayTree> = match db.get(CATALOG_INDEX_KEY) {
            Ok(v) => serde_cbor::from_slice(&v).unwrap(),
            Err(_) => HashSet::new(),
        };

        db.set(CATALOG_INDEX_KEY, &serde_cbor::to_vec(&series).unwrap());

        Catalog { db: db, series: series }
    }

    fn create(&mut self, name: String) -> &MWayTree {
        let key = self.db.add(&[]);
        let series = MWayTree { key: key, name: name.clone() };

        self.series.insert(series);

        self.db.set(CATALOG_INDEX_KEY, &serde_cbor::to_vec(&self.series).unwrap());

        return self.get_by_name(&name).unwrap()
    }

    fn delete(&mut self, series: MWayTree) {
        if self.series.contains(&series) {
            self.series.remove(&series);
            self.db.clear(series.key);
            self.db.set(CATALOG_INDEX_KEY, &serde_cbor::to_vec(&self.series).unwrap());
        }
    }

    fn get_by_name(&self, name: &String) -> Result<&MWayTree, &'static str> {
        for series in &self.series {
            if &series.name == name {
                return Ok(series)
            }
        }

        return Err("not found")
    }

    fn get_by_key(&self, key: u64) -> Result<&MWayTree, &'static str> {
        for series in &self.series {
            if series.key == key {
                return Ok(series)
            }
        }

        return Err("not found")
    }
}

#[derive(Debug)]
struct RocksDB
{
    db: DB,
    lastkey: u64,
}

impl RocksDB
{
    fn default() -> Self {
        RocksDB::open("lognplot.db".to_string())
    }

    fn open(path: String) -> Self {
        let db = DB::open_default(path).unwrap();
        let lastkey = db.latest_sequence_number();

        RocksDB { db: db, lastkey: lastkey }
    }

    fn next_key(&mut self) -> u64 {
        let key = self.lastkey + 1;
        self.lastkey = key;

        key
    }

    fn get(&self, key: u64) -> Result<Vec<u8>, &'static str> {
        //serde_cbor::from_slice(&value).unwrap()),
        match self.db.get(key.to_le_bytes()) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err("value not found"),
            Err(_) => Err("rocksdb problem encountered"),
        }
    }

    fn set(&mut self, key: u64, data: &[u8]) {
        //let data = serde_cbor::to_vec(&node).unwrap();
        self.db.put(key.to_le_bytes(), data);
    }

    fn add(&mut self, data: &[u8]) -> u64 {
        let key = self.next_key();
        self.set(key, data);

        key
    }

    fn clear(&mut self, key: u64) {
        self.db.delete(key.to_le_bytes());
    }
}

