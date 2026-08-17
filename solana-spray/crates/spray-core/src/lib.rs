//! Shared domain for the Restate-on-Solana experiment.
//!
//! Deliberately free of any Restate dependency: the workload is a plain async
//! Rust program, and `spray-service` decides which parts of it become durable
//! steps. Keeping the boundary here is what makes it possible to measure the
//! cost of durability by moving one knob.

pub mod config;
pub mod events;
pub mod hist;
pub mod lifecycle;
pub mod solana;
pub mod types;

pub use config::AppConfig;
