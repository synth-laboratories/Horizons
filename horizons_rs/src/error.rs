use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HorizonsErrorKind {
    NotFound,
    Auth,
    Validation,
    Server,
    Stream,
    Transport,
    Serialization,
}

#[derive(Debug, Error)]
#[error("{kind:?}: {message}")]
pub struct HorizonsError {
    pub kind: HorizonsErrorKind,
    pub status: Option<u16>,
    pub message: String,
}

impl HorizonsError {
    pub fn new(kind: HorizonsErrorKind, status: Option<u16>, message: impl Into<String>) -> Self {
        Self {
            kind,
            status,
            message: message.into(),
        }
    }
}

impl From<reqwest::Error> for HorizonsError {
    fn from(e: reqwest::Error) -> Self {
        HorizonsError::new(HorizonsErrorKind::Transport, None, e.to_string())
    }
}

impl From<serde_json::Error> for HorizonsError {
    fn from(e: serde_json::Error) -> Self {
        HorizonsError::new(HorizonsErrorKind::Serialization, None, e.to_string())
    }
}
