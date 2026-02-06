use crate::context_refresh::models::{EventTriggerEvent, RefreshTrigger};
use crate::context_refresh::traits::ContextRefresh;
use crate::models::{AgentIdentity, OrgId};
use crate::onboard::traits::{CentralDb, ListQuery};
use crate::{Error, Result};
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::sync::Arc;
use std::time::Duration;

/// Background scheduler for Context Refresh.
///
/// - Cron schedules are stored in `SourceConfig.schedule` (persisted in CentralDb).
/// - Event-driven triggers are stored in `SourceConfig.event_triggers`.
pub struct ContextRefreshScheduler {
    org_id: OrgId,
    central_db: Arc<dyn CentralDb>,
    refresh: Arc<dyn ContextRefresh>,
    poll_interval: Duration,
}

impl ContextRefreshScheduler {
    #[tracing::instrument(level = "debug", skip(central_db, refresh))]
    pub fn new(
        org_id: OrgId,
        central_db: Arc<dyn CentralDb>,
        refresh: Arc<dyn ContextRefresh>,
        poll_interval: Duration,
    ) -> Result<Self> {
        if poll_interval.is_zero() {
            return Err(Error::InvalidInput("poll_interval must be > 0".to_string()));
        }
        Ok(Self {
            org_id,
            central_db,
            refresh,
            poll_interval,
        })
    }

    /// Run the scheduler loop until the task is cancelled.
    #[tracing::instrument(level = "info", skip(self, identity))]
    pub async fn run_loop(&self, identity: AgentIdentity) -> Result<()> {
        let mut ticker = tokio::time::interval(self.poll_interval);
        loop {
            ticker.tick().await;
            // Best-effort tick; errors are logged but do not stop scheduling.
            if let Err(e) = self.tick(&identity, Utc::now()).await {
                tracing::warn!(error = %e, "context refresh scheduler tick failed");
            }
        }
    }

    /// Evaluate cron schedules and trigger any due refreshes.
    #[tracing::instrument(level = "debug", skip(self, identity))]
    pub async fn tick(&self, identity: &AgentIdentity, now: DateTime<Utc>) -> Result<()> {
        // Page through sources to avoid unbounded memory usage.
        let mut offset = 0usize;
        let limit = 200usize;
        loop {
            let batch = self
                .central_db
                .list_source_configs(self.org_id, ListQuery { limit, offset })
                .await?;

            if batch.is_empty() {
                break;
            }

            for mut source in batch {
                if !source.enabled {
                    continue;
                }
                let Some(schedule) = source.schedule.clone() else {
                    continue;
                };
                if now < schedule.next_run_at {
                    continue;
                }

                let cron = CronExpr::parse(&schedule.expr)?;
                // Compute from `now` (not the previous next_run_at) to avoid repeatedly scheduling in the past
                // after a prolonged downtime.
                let next = cron.next_after(now)?;
                source.schedule = Some(crate::context_refresh::models::CronSchedule {
                    expr: schedule.expr.clone(),
                    next_run_at: next,
                });
                source.touch(now);

                // Persist next_run_at before triggering to prevent duplicate ticks if we crash.
                self.central_db.upsert_source_config(&source).await?;

                let _ = self
                    .refresh
                    .trigger_refresh(
                        identity,
                        source.org_id,
                        &source.source_id,
                        RefreshTrigger::Cron {
                            schedule_id: schedule.expr.clone(),
                        },
                    )
                    .await;
            }

            offset += limit;
        }
        Ok(())
    }

    /// Handle an inbound event by triggering configured refreshes.
    #[tracing::instrument(level = "debug", skip(self, identity, event_payload))]
    pub async fn handle_inbound_event(
        &self,
        identity: &AgentIdentity,
        org_id: OrgId,
        event_source: &str,
        topic: &str,
        event_payload: &serde_json::Value,
    ) -> Result<u64> {
        if org_id != self.org_id {
            return Err(Error::Unauthorized(
                "scheduler org_id does not match requested org_id".to_string(),
            ));
        }
        if event_source.trim().is_empty() {
            return Err(Error::InvalidInput("event_source is empty".to_string()));
        }
        if topic.trim().is_empty() {
            return Err(Error::InvalidInput("topic is empty".to_string()));
        }

        let mut triggered = 0u64;
        let mut offset = 0usize;
        let limit = 200usize;
        loop {
            let batch = self
                .central_db
                .list_source_configs(org_id, ListQuery { limit, offset })
                .await?;
            if batch.is_empty() {
                break;
            }

            for source in batch {
                if !source.enabled {
                    continue;
                }
                if !source.event_triggers.iter().any(|cfg| {
                    if let Some(src) = &cfg.source {
                        if src != event_source {
                            return false;
                        }
                    }
                    if !topic_matches(&cfg.topic_pattern, topic) {
                        return false;
                    }
                    cfg.filter
                        .as_ref()
                        .map(|f| json_contains(event_payload, f))
                        .unwrap_or(true)
                }) {
                    continue;
                }

                let _ = self
                    .refresh
                    .trigger_refresh(
                        identity,
                        org_id,
                        &source.source_id,
                        RefreshTrigger::Event(EventTriggerEvent {
                            source: event_source.to_string(),
                            topic: topic.to_string(),
                        }),
                    )
                    .await;
                triggered += 1;
            }

            offset += limit;
        }
        Ok(triggered)
    }
}

/// Best-effort subset check: returns true if `filter` is structurally contained in `value`.
#[tracing::instrument(level = "debug")]
pub fn json_contains(value: &serde_json::Value, filter: &serde_json::Value) -> bool {
    match (value, filter) {
        (_, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => a == b,
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a == b,
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            // Each filter element must be contained in at least one value element.
            b.iter().all(|fb| a.iter().any(|av| json_contains(av, fb)))
        }
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => b
            .iter()
            .all(|(k, fv)| a.get(k).map(|av| json_contains(av, fv)).unwrap_or(false)),
        _ => false,
    }
}

#[tracing::instrument(level = "debug")]
pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    // Simple glob: '*' matches any substring (including dots), '?' matches one char.
    let mut pi = 0usize;
    let mut ti = 0usize;
    let p = pattern.as_bytes();
    let t = topic.as_bytes();
    let mut star: Option<usize> = None;
    let mut star_match: usize = 0;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == t[ti] || p[pi] == b'?') {
            pi += 1;
            ti += 1;
            continue;
        }
        if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            pi += 1;
            star_match = ti;
            continue;
        }
        if let Some(s) = star {
            pi = s + 1;
            star_match += 1;
            ti = star_match;
            continue;
        }
        return false;
    }

    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

/// Parsed cron expression supporting 5-field schedules: "min hour dom month dow".
///
/// Supported tokens per field:
/// - `*` all values
/// - `*/N` steps
/// - `A-B` inclusive range
/// - `A,B,C` list
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpr {
    minute: Field,
    hour: Field,
    day_of_month: Field,
    month: Field,
    day_of_week: Field,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    min: u32,
    max: u32,
    allowed: Vec<bool>,
}

impl Field {
    fn new(min: u32, max: u32) -> Self {
        let size = (max - min + 1) as usize;
        Self {
            min,
            max,
            allowed: vec![false; size],
        }
    }

    fn set(&mut self, v: u32) -> Result<()> {
        if v < self.min || v > self.max {
            return Err(Error::InvalidInput(format!(
                "cron field value {v} out of range {}..={}",
                self.min, self.max
            )));
        }
        self.allowed[(v - self.min) as usize] = true;
        Ok(())
    }

    fn set_all(&mut self) {
        for a in &mut self.allowed {
            *a = true;
        }
    }

    fn matches(&self, v: u32) -> bool {
        if v < self.min || v > self.max {
            return false;
        }
        self.allowed[(v - self.min) as usize]
    }
}

impl CronExpr {
    #[tracing::instrument(level = "debug")]
    pub fn parse(expr: &str) -> Result<Self> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(Error::InvalidInput(
                "cron expr must have 5 fields: min hour dom month dow".to_string(),
            ));
        }

        Ok(Self {
            minute: parse_field(parts[0], 0, 59)?,
            hour: parse_field(parts[1], 0, 23)?,
            day_of_month: parse_field(parts[2], 1, 31)?,
            month: parse_field(parts[3], 1, 12)?,
            day_of_week: parse_field(parts[4], 0, 6)?,
        })
    }

    #[tracing::instrument(level = "debug")]
    pub fn matches(&self, t: DateTime<Utc>) -> bool {
        let minute = t.minute();
        let hour = t.hour();
        let dom = t.day();
        let month = t.month();
        let dow = t.weekday().num_days_from_sunday();

        self.minute.matches(minute)
            && self.hour.matches(hour)
            && self.day_of_month.matches(dom)
            && self.month.matches(month)
            && self.day_of_week.matches(dow)
    }

    /// Return the next time at or after `after`, strictly after by one minute, that matches.
    #[tracing::instrument(level = "debug")]
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
        // Round to next minute boundary.
        let mut t = after + chrono::Duration::minutes(1);
        t = t
            .with_second(0)
            .and_then(|d| d.with_nanosecond(0))
            .unwrap_or(t);

        // Search up to 366 days to avoid infinite loops on impossible schedules.
        for _ in 0..(366 * 24 * 60) {
            if self.matches(t) {
                return Ok(t);
            }
            t = t + chrono::Duration::minutes(1);
        }
        Err(Error::InvalidInput(
            "cron expr produced no matching time within 366 days".to_string(),
        ))
    }
}

fn parse_field(token: &str, min: u32, max: u32) -> Result<Field> {
    let mut f = Field::new(min, max);
    if token == "*" {
        f.set_all();
        return Ok(f);
    }

    for part in token.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(Error::InvalidInput("empty cron field token".to_string()));
        }
        if let Some(step) = part.strip_prefix("*/") {
            let n: u32 = step
                .parse()
                .map_err(|_| Error::InvalidInput("invalid cron step".to_string()))?;
            if n == 0 {
                return Err(Error::InvalidInput("cron step must be > 0".to_string()));
            }
            let mut v = min;
            while v <= max {
                f.set(v)?;
                v += n;
            }
            continue;
        }
        if let Some((a, b)) = part.split_once('-') {
            let start: u32 = a
                .parse()
                .map_err(|_| Error::InvalidInput("invalid cron range start".to_string()))?;
            let end: u32 = b
                .parse()
                .map_err(|_| Error::InvalidInput("invalid cron range end".to_string()))?;
            if start > end {
                return Err(Error::InvalidInput("cron range start > end".to_string()));
            }
            for v in start..=end {
                f.set(v)?;
            }
            continue;
        }

        let v: u32 = part
            .parse()
            .map_err(|_| Error::InvalidInput("invalid cron value".to_string()))?;
        f.set(v)?;
    }
    Ok(f)
}
