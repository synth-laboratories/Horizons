//! Optional connectors implementing the Context Refresh `Connector` trait.
//!
//! See: specifications/horizons/specs/onboard_business_logic.md

#[cfg(feature = "jira")]
pub mod jira;
#[cfg(feature = "linkedin")]
pub mod linkedin;
