mod kv;
mod error;

pub use kv::KvStore;
pub use error::{KvsError, Result};

#[cfg(test)]
mod tests {
    use crate::KvStore;
    use std::path::{PathBuf, Path};

    #[test]
    fn kvTest() {
        let mut path = PathBuf::from("/");
        path.push("Users");
        path.push("kous");
        path.push("Desktop");
        path.push("res");
        path.push("database");

        let mut store = KvStore::open(path).expect("数据库不存在");
        for i in (1..100000).rev() {
            let key : String = "key".to_owned() + &i.to_string();
            let value : String = "value:value：value：value：value：value：value：".to_owned() + &i.to_string();
            store.set(key, value);
        }

        let result = store.get("key333".to_string()).expect("None").expect("None");

        println!("{}", result);
    }
}