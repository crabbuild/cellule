use super::Config;

fn config() -> Config {
    serde_json::from_value(serde_json::json!({
        "address": "127.0.0.1:18080",
        "node_addresses": ["127.0.0.1:18080", "127.0.0.1:18081", "127.0.0.1:18082"],
        "cells": 2000, "concurrency": 64, "queue_capacity": 4096,
        "write_rate": 100, "read_rate": 0, "warmup_seconds": 10, "seconds": 60,
        "evidence_directory": "/tmp/test-evidence"
    }))
    .unwrap()
}

#[test]
fn global_shards_route_to_their_static_owner() {
    let config = config();
    config.validate().unwrap();
    assert_eq!(config.url(0), "http://127.0.0.1:18080");
    assert_eq!(config.url(1), "http://127.0.0.1:18081");
    assert_eq!(config.url(2), "http://127.0.0.1:18082");
    assert_eq!(config.url(1999), "http://127.0.0.1:18081");
}

#[test]
fn reject_repeated_or_nonlocal_endpoints_and_preserve_single_node_config() {
    let mut config = config();
    config.node_addresses[1] = config.node_addresses[0];
    assert!(config.validate().is_err());
    config.node_addresses = vec!["192.0.2.1:18080".parse().unwrap()];
    assert!(config.validate().is_err());
    config.node_addresses.clear();
    config.validate().unwrap();
    assert_eq!(config.url(1999), "http://127.0.0.1:18080");
}
