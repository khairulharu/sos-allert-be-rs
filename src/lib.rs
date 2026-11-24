//! SOS alert backend (Rust port).
//!
//! Clean architecture layout mirrors the Go original
//! (`khairulharu/sos-allert-be`):
//!
//! * [`domain`]      - persistence entities, no logic
//! * [`repository`]  - storage traits + implementations (postgres, redis)
//! * [`usecase`]     - business rules, owns transactions
//! * [`delivery`]    - http handlers/middleware and kafka consumers
//! * [`gateway`]     - outbound integrations (messaging producers, notifiers)
//! * [`config`]      - config.json loading and dependency wiring inputs
//!
//! Unit tests live next to their module in a `tests/` child module so the
//! logic file stays free of test code; cross-module tests live in `tests/`.

pub mod bootstrap;
pub mod config;
pub mod delivery;
pub mod domain;
pub mod error;
pub mod gateway;
pub mod geo;
pub mod model;
pub mod repository;
pub mod storage;
pub mod tracking;
pub mod usecase;

pub use error::{AppError, AppResult};
