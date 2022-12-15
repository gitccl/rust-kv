use std::{env::current_dir, process::exit};

use clap::{arg, Command};
use rust_kv::{KvStore, Result, KvError};

fn main() -> Result<()> {
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

    let mut kv = KvStore::open(current_dir()?)?;

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let value = sub_matches.get_one::<String>("value").unwrap().clone();
            kv.set(key, value)?;
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let value = kv.get(key)?;
            if let Some(value) = value {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            match kv.remove(key) {
                Ok(()) => {},
                Err(KvError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                },
                Err(e) => {
                    return Err(e)
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
