use std::io::Write;

use clap::{arg, Command};
use rust_kv::{KvClient, KvError, Result};
use tokio::io::{AsyncBufReadExt, BufReader};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .disable_help_subcommand(true)
        .arg(
            arg!(--addr <IP_PORT> "The address of the server")
                .default_value(DEFAULT_LISTENING_ADDRESS),
        )
        .get_matches();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let addr = matches.get_one::<String>("addr").unwrap().clone();
        let mut client = KvClient::new(addr).await?;
        let mut line_reader = BufReader::new(tokio::io::stdin()).lines();
        println!("Use \\help to get usage.");
        loop {
            print!("> ");
            std::io::stdout().flush().unwrap();

            let line = match line_reader.next_line().await? {
                Some(line) => line,
                None => break,
            };

            if line == "q" || line == "exit" {
                println!("client exited...");
                break;
            } else if line == "\\help" {
                println!("set <key> <value>: set the value of a string key");
                println!("get <key>: get the string value of a given string key");
                println!("rm <key>: remove a given key");
                println!("exit: exit the client");
            }

            let inputs: Vec<&str> = line.split(' ').collect();
            if inputs.len() < 2 {
                continue;
            }
            match inputs[0] {
                "set" => {
                    if inputs.len() != 3 {
                        println!("invalid set command");
                    }
                    let key = inputs[1].to_string();
                    let value = inputs[2].to_string();
                    match client.set(key, value).await {
                        Ok(_) => println!("Ok"),
                        Err(err) => println!("Error: {}", err),
                    }
                }
                "get" => {
                    let key = inputs[1].to_string();
                    match client.get(key).await {
                        Ok(Some(value)) => println!("{}", value),
                        Ok(None) => println!("Key not found"),
                        Err(err) => println!("Error: {}", err),
                    }
                }
                "rm" => {
                    let key = inputs[1].to_string();
                    match client.remove(key).await {
                        Ok(_) => println!("Ok"),
                        Err(err) => println!("Error: {}", err),
                    }
                }
                _ => {
                    println!("unknown command");
                }
            };
        }
        Ok::<(), KvError>(())
    })
}
