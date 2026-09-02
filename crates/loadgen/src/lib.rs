//! Burst-create load generator for the AgentENV HTTP API.
//!
//! The generator speaks the same E2B-compatible API the e2e suites do, so one
//! binary drives a node directly or a gateway in front of a fleet. Against a
//! node running `[machine].backend = "mock"` it exercises the whole control
//! plane — admission, orchestrator state machine, scheduler binding, proxy
//! route table — on a host with no hypervisor.
//!
//! The measurement pieces (arrival schedule, quantiles, error taxonomy) are
//! separated from the HTTP driver so they are testable without a server, and
//! so the numbers a run reports can be checked rather than trusted.

pub mod arrivals;
pub mod driver;
pub mod report;

pub use arrivals::PoissonArrivals;
pub use driver::{Driver, LoadPlan, Mode, Source, Target};
pub use report::{Outcome, Quantiles, RequestRecord, Stage, Tally};
