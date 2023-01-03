use assert_cmd::prelude::*;
use predicates::str::contains;
use std::fs::{self, File};
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn cli_log_configuration() {
    let temp_dir = TempDir::new().unwrap();
    let stderr_path = temp_dir.path().join("stderr");
    let mut cmd = Command::cargo_bin("kv-server").unwrap();
    let mut child = cmd
        .args(&["--engine", "kvs", "--addr", "127.0.0.1:4001"])
        .current_dir(&temp_dir)
        .stderr(File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("server exited before killed");

    let content = fs::read_to_string(&stderr_path).expect("unable to read from stderr file");
    assert!(content.contains(env!("CARGO_PKG_VERSION")));
    assert!(content.contains("kvs"));
    assert!(content.contains("127.0.0.1:4001"));
}

#[test]
fn cli_wrong_engine() {
    // sled first, kvs second
    {
        let temp_dir = TempDir::new().unwrap();
        let mut cmd = Command::cargo_bin("kv-server").unwrap();
        let mut child = cmd
            .args(&["--engine", "sled", "--addr", "127.0.0.1:4002"])
            .current_dir(&temp_dir)
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        child.kill().expect("server exited before killed");

        let mut cmd = Command::cargo_bin("kv-server").unwrap();
        cmd.args(&["--engine", "kvs", "--addr", "127.0.0.1:4003"])
            .current_dir(&temp_dir)
            .assert()
            .failure();
    }

    // kvs first, sled second
    {
        let temp_dir = TempDir::new().unwrap();
        let mut cmd = Command::cargo_bin("kv-server").unwrap();
        let mut child = cmd
            .args(&["--engine", "kvs", "--addr", "127.0.0.1:4002"])
            .current_dir(&temp_dir)
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        child.kill().expect("server exited before killed");

        let mut cmd = Command::cargo_bin("kv-server").unwrap();
        cmd.args(&["--engine", "sled", "--addr", "127.0.0.1:4003"])
            .current_dir(&temp_dir)
            .assert()
            .failure();
    }
}

fn cli_access_server(engine: &str, addr: &str) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let temp_dir = TempDir::new().unwrap();
    let mut server = Command::cargo_bin("kv-server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });

    thread::sleep(Duration::from_secs(1));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("set key1 value1")
        .assert()
        .success()
        .stdout(contains("Ok"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("get key1")
        .assert()
        .success()
        .stdout(contains("value1"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("set key1 value2")
        .assert()
        .success()
        .stdout(contains("Ok"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("get key1")
        .assert()
        .success()
        .stdout(contains("value2"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("get key2")
        .assert()
        .success()
        .stdout(contains("Key not found"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("rm key2")
        .assert()
        .success()
        .stdout(contains("Key not found"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("set key2 value3")
        .assert()
        .success()
        .stdout(contains("Ok"));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("rm key1")
        .assert()
        .success()
        .stdout(contains("Ok"));

    sender.send(()).unwrap();
    handle.join().unwrap();

    // Reopen and check value
    let (sender, receiver) = mpsc::sync_channel(0);
    let mut server = Command::cargo_bin("kv-server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("get key2")
        .assert()
        .success()
        .stdout(contains("value3"));
    assert_cmd::Command::cargo_bin("kv-client")
        .unwrap()
        .args(&["--addr", addr])
        .current_dir(&temp_dir)
        .write_stdin("get key1")
        .assert()
        .success()
        .stdout(contains("Key not found"));
    sender.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
fn cli_access_server_kvs_engine() {
    cli_access_server("kvs", "127.0.0.1:4004");
}

#[test]
fn cli_access_server_sled_engine() {
    cli_access_server("sled", "127.0.0.1:4005");
}
