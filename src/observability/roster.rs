//! Deciding whether a heartbeat needs to carry the node's sandbox roster.
//!
//! A node's roster barely changes: the set of sandboxes it owns moves on
//! creates and deletes, while heartbeats go out every few seconds. Sending the
//! whole set each time is the largest part of a heartbeat — two hundred
//! sandboxes is several kilobytes of UUIDs, and a fleet of ten thousand such
//! nodes spends tens of megabytes a second repeating itself.
//!
//! So the node sends a digest every time and the roster only when it has to.
//!
//! # Never eliding into the dark
//!
//! An elided roster and an empty one are the same bytes on the wire and mean
//! opposite things: "unchanged" versus "this node owns nothing, delete its
//! bindings". A scheduler that does not understand digests would read the
//! first as the second and wipe the node's entire data plane.
//!
//! The node therefore sends the full roster until a scheduler has told it, in
//! a response, that digests are understood. `request_full_roster` cannot carry
//! that: false there means "no need", which is also what an older scheduler
//! sends. So it takes its own field, and the default — send everything — is
//! the safe one.
//!
//! That answer is a property of the process that gave it, not of the
//! deployment. Schedulers are replaced by rollouts and rollbacks, and a node
//! can reach a different one without ever seeing an error — so the permission
//! to elide expires with every response and has to be renewed by the next.
//!
//! # Saying so on the wire
//!
//! Even that is not quite enough on its own, because it only governs the
//! heartbeat after the one that discovers the change. A heartbeat already in
//! flight cannot know which scheduler will receive it.
//!
//! So an elided heartbeat also stops claiming its ids are authoritative:
//! `roster_complete` describes the message, not just the node. Carrying no
//! roster and simultaneously asserting the node holds nothing is a false
//! statement, and it is precisely the statement an older scheduler acts on.
//! Withdrawing it means a misread elision reconciles nothing instead of
//! deleting everything.

use sha2::{Digest, Sha256};

use crate::types::SandboxId;

/// Tracks what the scheduler has been told, so the next heartbeat can decide
/// what it must repeat.
#[derive(Debug, Default)]
pub struct RosterDigestState {
    /// The roster the scheduler has been sent in full, if any.
    acknowledged: Option<Acknowledged>,
    /// Whether the most recent response said digests are understood.
    scheduler_understands_digests: bool,
    /// Whether the scheduler has asked for the roster back.
    full_roster_requested: bool,
}

/// A roster this node has sent in full, and the terms it sent it on.
#[derive(Debug, PartialEq, Eq)]
struct Acknowledged {
    digest: String,
    /// Whether that send claimed the roster was the node's authoritative view.
    ///
    /// Part of the identity of what the scheduler holds, not a detail of how
    /// it was sent: the same ids mean different things depending on it, and
    /// the scheduler caches the answer. A node that finishes startup recovery
    /// without its roster changing must therefore say so in full rather than
    /// elide, or the scheduler keeps treating a now-authoritative roster as
    /// provisional and never reaps what the node has stopped holding.
    authoritative: bool,
}

/// What one heartbeat should carry.
#[derive(Debug, PartialEq, Eq)]
pub struct RosterReport {
    pub digest: String,
    /// `None` means the roster is elided and the digest stands in for it.
    pub sandbox_ids: Option<Vec<String>>,
    /// Whether the ids in this heartbeat are the node's authoritative view.
    ///
    /// False for every elided heartbeat, whatever the node's own state: there
    /// are no ids in it to be authoritative about.
    pub roster_complete: bool,
}

impl RosterDigestState {
    /// Records what a scheduler said about the roster it just received.
    pub fn observe_response(&mut self, digest_accepted: bool, full_roster_requested: bool) {
        // Assignment, not accumulation. This is the answer from the process
        // that just replied, and the next reply may come from an older one
        // that would read an elided roster as an empty one.
        self.scheduler_understands_digests = digest_accepted;
        self.full_roster_requested = full_roster_requested;
    }

    /// Forgets what any scheduler knows.
    ///
    /// Called when the connection to the scheduler is re-established: the
    /// process on the other end may be a different one that has never seen
    /// this node's roster, and it has no way to tell us so before we have
    /// already elided.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Decides what the next heartbeat carries.
    ///
    /// `authoritative` is the node's own claim — whether startup recovery has
    /// finished and its roster is therefore the whole truth about what it
    /// holds. It gates what the heartbeat may assert, and a change in it
    /// forces a full send even when the ids have not moved.
    pub fn report(&mut self, sandbox_ids: &[SandboxId], authoritative: bool) -> RosterReport {
        let digest = roster_digest(sandbox_ids);
        let sent = Acknowledged {
            digest,
            authoritative,
        };

        let may_elide = self.scheduler_understands_digests
            && !self.full_roster_requested
            && self.acknowledged.as_ref() == Some(&sent);

        if may_elide {
            return RosterReport {
                digest: sent.digest,
                sandbox_ids: None,
                roster_complete: false,
            };
        }

        let report = RosterReport {
            digest: sent.digest.clone(),
            sandbox_ids: Some(sandbox_ids.iter().map(SandboxId::to_string).collect()),
            roster_complete: authoritative,
        };
        // Recorded as acknowledged optimistically. If the heartbeat never
        // lands, the next response either asks for the roster back or the
        // connection resets, and both paths send it again.
        self.acknowledged = Some(sent);
        report
    }
}

/// Hashes a roster.
///
/// Sorted first, so the digest depends on the set and not on the order the ids
/// happened to come out of the metadata store — an order that is not stable
/// and that neither side should have to promise.
pub fn roster_digest(sandbox_ids: &[SandboxId]) -> String {
    let mut ids: Vec<String> = sandbox_ids.iter().map(SandboxId::to_string).collect();
    ids.sort_unstable();

    let mut hasher = Sha256::new();
    for id in &ids {
        hasher.update(id.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(count: usize) -> Vec<SandboxId> {
        (0..count).map(|_| SandboxId::new()).collect()
    }

    fn accepted() -> RosterDigestState {
        let mut state = RosterDigestState::default();
        state.observe_response(true, false);
        state
    }

    #[test]
    fn the_digest_depends_on_the_set_not_the_order() {
        let mut roster = ids(5);
        let forward = roster_digest(&roster);
        roster.reverse();
        assert_eq!(roster_digest(&roster), forward);

        roster.push(SandboxId::new());
        assert_ne!(roster_digest(&roster), forward);
    }

    /// The digest is a wire format shared with the Go scheduler, so it is
    /// pinned to fixed vectors rather than compared against itself. The same
    /// vectors are asserted in `services/scheduler/internal/roster_cache_test.go`;
    /// if the two ever disagree, every heartbeat silently becomes a re-send.
    #[test]
    fn the_digest_matches_the_agreed_wire_format() {
        // sha256 over sorted ids, each followed by a newline.
        assert_eq!(
            digest_of(&["c", "a", "b"]),
            "880553fca8fcea94e325ee2cfb48e5a985cc797f39a14cc6d3cedecfeb2ae4d2"
        );
        // An empty roster is sha256 of nothing, not the empty string.
        assert_eq!(
            roster_digest(&[]),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Digests a roster of arbitrary strings, so the wire-format vectors do
    /// not have to be valid sandbox ids.
    fn digest_of(ids: &[&str]) -> String {
        let mut sorted: Vec<&str> = ids.to_vec();
        sorted.sort_unstable();
        let mut hasher = Sha256::new();
        for id in sorted {
            hasher.update(id.as_bytes());
            hasher.update(b"\n");
        }
        hex::encode(hasher.finalize())
    }

    /// The safe default. A scheduler that has said nothing might be one that
    /// would read an elided roster as an empty one and delete every binding.
    #[test]
    fn the_roster_is_sent_until_a_scheduler_says_it_understands_digests() {
        let mut state = RosterDigestState::default();
        let roster = ids(3);

        for _ in 0..3 {
            let report = state.report(&roster, true);
            assert!(
                report.sandbox_ids.is_some(),
                "the roster must not be elided before the scheduler has said it can be"
            );
        }
    }

    #[test]
    fn an_unchanged_roster_is_elided_once_the_scheduler_has_it() {
        let mut state = accepted();
        let roster = ids(3);

        let first = state.report(&roster, true);
        assert_eq!(first.sandbox_ids.as_ref().map(Vec::len), Some(3));

        let second = state.report(&roster, true);
        assert_eq!(second.sandbox_ids, None);
        assert_eq!(second.digest, first.digest);
    }

    #[test]
    fn a_changed_roster_is_always_sent() {
        let mut state = accepted();
        let roster = ids(3);
        state.report(&roster, true);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);

        let mut grown = roster.clone();
        grown.push(SandboxId::new());
        let report = state.report(&grown, true);
        assert_eq!(report.sandbox_ids.as_ref().map(Vec::len), Some(4));

        // And a roster that empties out, which is the case a scheduler most
        // needs to hear about and the one an elision would hide.
        let report = state.report(&[], true);
        assert_eq!(report.sandbox_ids, Some(Vec::new()));
    }

    /// The scheduler restarted and holds nothing for this node. If the node
    /// kept eliding because its roster had not changed, the scheduler would
    /// hold no bindings for it indefinitely.
    #[test]
    fn a_requested_roster_is_sent_even_when_unchanged() {
        let mut state = accepted();
        let roster = ids(3);
        state.report(&roster, true);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);

        state.observe_response(true, true);
        assert!(state.report(&roster, true).sandbox_ids.is_some());

        // And elision resumes once the request is satisfied.
        state.observe_response(true, false);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);
    }

    /// A reconnect may reach a different scheduler process that has never seen
    /// this node, and it cannot tell us so until after we have already elided.
    #[test]
    fn reconnecting_resends_the_roster() {
        let mut state = accepted();
        let roster = ids(3);
        state.report(&roster, true);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);

        state.reset();
        assert!(
            state.report(&roster, true).sandbox_ids.is_some(),
            "a reconnected node must reintroduce itself"
        );
    }

    /// A rollout or rollback puts an older scheduler in front of a node that
    /// has already learned to elide. It answers `roster_digest_accepted=false`
    /// because it has never heard of the field, and the node has to believe it
    /// — the alternative is eliding to a process that reads an elided roster
    /// as an empty one and deletes every binding on the node.
    ///
    /// The permission to elide therefore expires with each response instead of
    /// latching on the first one that granted it.
    #[test]
    fn a_scheduler_that_stops_understanding_digests_stops_the_eliding() {
        let mut state = accepted();
        let roster = ids(3);
        state.report(&roster, true);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);

        state.observe_response(false, false);
        for _ in 0..3 {
            assert!(
                state.report(&roster, true).sandbox_ids.is_some(),
                "an older scheduler must be sent the roster every time"
            );
        }

        // And the node recovers when a newer one is in front of it again.
        state.observe_response(true, false);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);
    }

    /// The same skew, reached through the path that does surface an error: the
    /// reconnect resets the node, the full roster goes to the older scheduler,
    /// and its response must not re-arm the elision that the optimistic
    /// acknowledgement in `report` has just recorded.
    #[test]
    fn reconnecting_into_an_older_scheduler_keeps_sending_the_roster() {
        let mut state = accepted();
        let roster = ids(3);
        state.report(&roster, true);
        assert_eq!(state.report(&roster, true).sandbox_ids, None);

        state.reset();
        assert!(state.report(&roster, true).sandbox_ids.is_some());
        state.observe_response(false, false);

        assert!(
            state.report(&roster, true).sandbox_ids.is_some(),
            "the roster must keep going out to a scheduler that never acknowledged it"
        );
    }

    /// An elided heartbeat carries no ids, so it cannot claim its ids are the
    /// node's whole truth. Saying otherwise is what lets a scheduler that
    /// cannot resolve the digest read the message as "this node holds
    /// nothing".
    #[test]
    fn an_elided_heartbeat_does_not_claim_to_be_authoritative() {
        let mut state = accepted();
        let roster = ids(3);

        let full = state.report(&roster, true);
        assert!(full.sandbox_ids.is_some());
        assert!(full.roster_complete, "a full roster says what it is");

        let elided = state.report(&roster, true);
        assert_eq!(elided.sandbox_ids, None);
        assert!(
            !elided.roster_complete,
            "an elided roster must not assert authority over ids it did not send"
        );
    }

    /// A node still replaying its own state at startup may hold sandboxes it
    /// has not found yet, so its roster is not authoritative and it says so.
    /// The scheduler caches that answer alongside the ids, which means the
    /// node cannot let recovery finish silently: the ids are unchanged, but
    /// what they mean is not.
    #[test]
    fn finishing_recovery_resends_an_unchanged_roster() {
        let mut state = accepted();
        let roster = ids(3);

        let recovering = state.report(&roster, false);
        assert!(recovering.sandbox_ids.is_some());
        assert!(!recovering.roster_complete);
        assert_eq!(state.report(&roster, false).sandbox_ids, None);

        let recovered = state.report(&roster, true);
        assert!(
            recovered.sandbox_ids.is_some(),
            "becoming authoritative must be said in full, not elided"
        );
        assert!(recovered.roster_complete);
        assert_eq!(recovered.digest, recovering.digest);

        // And it settles again on the new terms.
        assert_eq!(state.report(&roster, true).sandbox_ids, None);
    }
}
