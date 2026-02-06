//! System 1: Context Refresh (automated data ingestion).
//!
//! This module defines a general-purpose connector interface and a refresh engine that:
//! - Pulls raw records incrementally from external systems (via a `Connector`)
//! - Processes them into normalized `ContextEntity` objects
//! - Stores them in a project-scoped database (via `ProjectDb`)
//! - Updates durable sync cursors (via `CentralDb::upsert_sync_state`)
//! - Publishes a `context.refreshed` event (via `horizons_events::EventBus`)

pub mod engine;
pub mod models;
pub mod schedule;
pub mod traits;
