//! What a run records and what it reports.

use std::collections::BTreeMap;
use std::collections::HashSet;

use serde::Serialize;

/// The stages one request walks through.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// `POST /sandboxes` returning 201 and an `x-agentenv-sandbox-id` header.
    Create,
    /// `GET /sandboxes/{id}` polled until the sandbox reports `running`.
    Ready,
    /// The first proxied request that succeeds.
    Proxy,
    /// `DELETE /sandboxes/{id}` cleanup, measured but not part of the offered
    /// load.
    Cleanup,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Ready => "ready",
            Self::Proxy => "proxy",
            Self::Cleanup => "cleanup",
        }
    }
}

/// How one request ended.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
    /// Every stage the plan asked for completed.
    Ok,
    /// A stage returned an HTTP status the generator does not accept.
    Status { stage: Stage, status: u16 },
    /// A stage never produced a status: connection refused, reset, or the
    /// request deadline elapsed.
    Transport { stage: Stage, error: String },
    /// An open-loop arrival that found the in-flight limit full. Counted
    /// separately: dropping arrivals silently turns an open loop into a closed
    /// one and hides the saturation the run exists to find.
    Shed,
}

/// One request's measurements, emitted as one line of newline-delimited JSON.
#[derive(Clone, Debug, Serialize)]
pub struct RequestRecord {
    pub seq: u64,
    /// Set once the node has answered a create with 201; its presence is what
    /// makes a later 404 the run's own fault rather than a bad request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_ms: Option<f64>,
    pub outcome: Outcome,
}

impl RequestRecord {
    pub fn new(seq: u64) -> Self {
        Self {
            seq,
            sandbox_id: None,
            create_ms: None,
            ready_ms: None,
            proxy_ms: None,
            outcome: Outcome::Ok,
        }
    }
}

/// Latency quantiles over one stage, in milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Quantiles {
    pub count: usize,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

impl Quantiles {
    /// Computes nearest-rank quantiles over `samples`.
    ///
    /// Nearest rank rather than interpolation: every reported number is a
    /// latency some request actually had, which is what a reader comparing a
    /// p99 against a timeout needs.
    pub fn from_samples(samples: &mut [f64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_by(f64::total_cmp);
        Self {
            count: samples.len(),
            p50: nearest_rank(samples, 0.50),
            p90: nearest_rank(samples, 0.90),
            p99: nearest_rank(samples, 0.99),
            max: samples[samples.len() - 1],
        }
    }
}

fn nearest_rank(sorted: &[f64], quantile: f64) -> f64 {
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

/// Accumulates every record a run produces into the summary it prints.
#[derive(Debug, Default)]
pub struct Tally {
    create_ms: Vec<f64>,
    ready_ms: Vec<f64>,
    proxy_ms: Vec<f64>,
    created: HashSet<String>,
    completed: u64,
    shed: u64,
    statuses: BTreeMap<(Stage, u16), u64>,
    transport: BTreeMap<Stage, u64>,
    bad_gateway: u64,
    self_inflicted_404: u64,
}

impl Tally {
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one request's record into the summary.
    pub fn observe(&mut self, record: &RequestRecord) {
        if let Some(id) = &record.sandbox_id {
            self.created.insert(id.clone());
        }
        if let Some(ms) = record.create_ms {
            self.create_ms.push(ms);
        }
        if let Some(ms) = record.ready_ms {
            self.ready_ms.push(ms);
        }
        if let Some(ms) = record.proxy_ms {
            self.proxy_ms.push(ms);
        }

        match &record.outcome {
            Outcome::Ok => self.completed += 1,
            Outcome::Shed => self.shed += 1,
            Outcome::Transport { stage, .. } => {
                *self.transport.entry(*stage).or_default() += 1;
            }
            Outcome::Status { stage, status } => {
                *self.statuses.entry((*stage, *status)).or_default() += 1;
                if *status == 502 {
                    self.bad_gateway += 1;
                }
                // A 404 only counts against the control plane when the node
                // has already handed this run the id: that is the signature of
                // a sandbox created without a scheduler binding, or of a
                // binding reconciled away underneath a live sandbox. A 404 on
                // the create itself is a bad request, not a lost binding.
                if *status == 404 && record.sandbox_id.is_some() {
                    self.self_inflicted_404 += 1;
                }
            }
        }
    }

    /// Renders the summary, given how long the offered load ran for.
    pub fn summary(&mut self, elapsed_secs: f64) -> Summary {
        let mut statuses = BTreeMap::new();
        for ((stage, status), count) in &self.statuses {
            statuses.insert(format!("{}:{status}", stage.as_str()), *count);
        }
        for (stage, count) in &self.transport {
            statuses.insert(format!("{}:transport", stage.as_str()), *count);
        }

        Summary {
            completed: self.completed,
            shed: self.shed,
            created: self.created.len() as u64,
            elapsed_secs,
            creates_per_sec: if elapsed_secs > 0.0 {
                self.created.len() as f64 / elapsed_secs
            } else {
                0.0
            },
            create: Quantiles::from_samples(&mut self.create_ms),
            ready: Quantiles::from_samples(&mut self.ready_ms),
            proxy: Quantiles::from_samples(&mut self.proxy_ms),
            errors: statuses,
            bad_gateway: self.bad_gateway,
            self_inflicted_404: self.self_inflicted_404,
        }
    }
}

/// The end-of-run report.
#[derive(Clone, Debug, Serialize)]
pub struct Summary {
    pub completed: u64,
    pub shed: u64,
    pub created: u64,
    pub elapsed_secs: f64,
    pub creates_per_sec: f64,
    pub create: Quantiles,
    pub ready: Quantiles,
    pub proxy: Quantiles,
    /// Counts keyed `"<stage>:<status>"`, plus `"<stage>:transport"`.
    pub errors: BTreeMap<String, u64>,
    /// 502s at any stage. A gateway turns an unreadable or over-limit upstream
    /// body into one of these after the node has already done the work.
    pub bad_gateway: u64,
    /// 404s on a sandbox this run was handed by a 201.
    pub self_inflicted_404: u64,
}

#[cfg(test)]
mod tests {
    use super::{Outcome, Quantiles, RequestRecord, Stage, Tally};

    #[test]
    fn quantiles_report_observed_latencies() {
        let mut samples: Vec<f64> = (1..=100).map(f64::from).collect();
        let quantiles = Quantiles::from_samples(&mut samples);

        assert_eq!(quantiles.count, 100);
        assert_eq!(quantiles.p50, 50.0);
        assert_eq!(quantiles.p90, 90.0);
        assert_eq!(quantiles.p99, 99.0);
        assert_eq!(quantiles.max, 100.0);
    }

    #[test]
    fn quantiles_of_an_empty_stage_are_zero() {
        assert_eq!(Quantiles::from_samples(&mut []), Quantiles::default());
    }

    /// The count that makes a lost binding visible must not be inflatable by
    /// a client-side mistake.
    ///
    /// "404 on a sandbox this run created" is the direct signature of a
    /// binding that was never recorded or was reconciled away; "404 because
    /// the request named a sandbox nobody created" is a bad request. Counting
    /// both under one name would make the number useless the first time a run
    /// was pointed at a stale id.
    #[test]
    fn only_a_404_on_an_own_sandbox_counts_against_the_control_plane() {
        let mut tally = Tally::new();

        // The node answered the create, then denied the sandbox exists.
        let mut ours = RequestRecord::new(1);
        ours.sandbox_id = Some("sbx-created-by-this-run".to_string());
        ours.create_ms = Some(12.0);
        ours.outcome = Outcome::Status {
            stage: Stage::Ready,
            status: 404,
        };
        tally.observe(&ours);

        // A 404 with no id in hand: the create itself was refused.
        let mut theirs = RequestRecord::new(2);
        theirs.outcome = Outcome::Status {
            stage: Stage::Create,
            status: 404,
        };
        tally.observe(&theirs);

        let summary = tally.summary(1.0);
        assert_eq!(summary.self_inflicted_404, 1);
        assert_eq!(summary.created, 1);
        assert_eq!(summary.errors.get("ready:404"), Some(&1));
        assert_eq!(summary.errors.get("create:404"), Some(&1));
    }

    #[test]
    fn bad_gateways_are_counted_at_whichever_stage_returned_them() {
        let mut tally = Tally::new();
        let mut record = RequestRecord::new(1);
        record.outcome = Outcome::Status {
            stage: Stage::Create,
            status: 502,
        };
        tally.observe(&record);

        let summary = tally.summary(1.0);
        assert_eq!(summary.bad_gateway, 1);
        assert_eq!(summary.self_inflicted_404, 0);
    }

    /// Throughput is per distinct sandbox the node acknowledged, so a retry
    /// that re-reports the same id cannot inflate it.
    #[test]
    fn creates_per_sec_counts_distinct_sandboxes() {
        let mut tally = Tally::new();
        for seq in 0..3 {
            let mut record = RequestRecord::new(seq);
            record.sandbox_id = Some("sbx-same".to_string());
            tally.observe(&record);
        }

        let summary = tally.summary(2.0);
        assert_eq!(summary.created, 1);
        assert_eq!(summary.creates_per_sec, 0.5);
    }
}
