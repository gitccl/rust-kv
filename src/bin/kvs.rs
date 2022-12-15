use std::env::current_dir;

use clap::{arg, Command};
use rust_kv::KvStore;

fn main() {
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .disable_help_subcommand(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to a string")
                .arg(arg!(<key> "A string key"))
                .arg(arg!(<value> "The string value of the key")),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<key> "A string key")),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a given key")
                .arg(arg!(<key> "A string key")),
        )
        .get_matches();

    let mut kv = KvStore::open(current_dir().unwrap());

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let value = sub_matches.get_one::<String>("value").unwrap().clone();
            kv.set(key, value);
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let value = kv.get(key);
            print!("{:?}", value);
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            kv.remove(key);
        }
        _ => unreachable!(),
    }
}
