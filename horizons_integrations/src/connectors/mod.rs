//! Optional connectors implementing the Context Refresh `Connector` trait.
//!
//! See: specifications/horizons/specs/onboard_business_logic.md

#[cfg(feature = "github")]
pub mod github;
#[cfg(feature = "gmail")]
pub mod gmail;
#[cfg(feature = "hubspot")]
pub mod hubspot;
#[cfg(feature = "jira")]
pub mod jira;
#[cfg(feature = "linear")]
pub mod linear;
#[cfg(feature = "linkedin")]
pub mod linkedin;
#[cfg(feature = "slack")]
pub mod slack;
