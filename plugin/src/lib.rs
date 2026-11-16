//! The host's Bluetooth as a Yoke Plugin, through BlueZ.
//!
//! `backend` talks to BlueZ and serves its pairing agent, and knows nothing of Yoke; `plugin` is the
//! Plugin around it.

pub mod backend;
pub mod plugin;
