package scheduler

import (
	"context"
	"encoding/json"
	"strings"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mobility RPCs.
//
// Nodes keep their paused-sandbox records here rather than on their own disks
// because the claim protocol they support is inherently cross-node: a
// destination takes a sandbox from an origin, and two nodes cannot arbitrate
// through a store that lives on one of them.
//
// The scheduler is deliberately incurious about what a record says. It
// enforces exactly one rule — the generation ordering — and stores the rest
// verbatim. Compatibility is a judgement only a candidate node can make, and a
// scheduler that tried to make it would have to be redeployed every time the
// fingerprint gained a field.

func (s *Service) UpsertMobilityRecord(
	ctx context.Context,
	req *schedulerv1.UpsertMobilityRecordRequest,
) (*schedulerv1.UpsertMobilityRecordResponse, error) {
	record, err := mobilityRecordFromProto(req.GetRecord())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Only nodes the scheduler knows about may write records, for the same
	// reason only they may move its placement view.
	if _, known := s.nodes.Resolve(record.OriginNodeID); !known {
		return nil, status.Error(codes.InvalidArgument, "origin node is not in scheduler node list")
	}

	// A request that states an expectation is arbitrating ownership and must
	// go through the compare-and-set; one that does not is bookkeeping.
	var applied bool
	if req.GetExpectAbsent() || strings.TrimSpace(req.GetExpectedGeneration()) != "" {
		applied, err = s.mobility.CompareAndSet(ctx, strings.TrimSpace(req.GetExpectedGeneration()), record)
	} else {
		applied, err = s.mobility.Upsert(ctx, record)
	}
	if err != nil {
		s.logger.Warn("scheduler mobility upsert failed",
			zap.String("sandbox_id", record.SandboxID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "mobility store unavailable")
	}
	return &schedulerv1.UpsertMobilityRecordResponse{Applied: applied}, nil
}

func (s *Service) GetMobilityRecord(
	ctx context.Context,
	req *schedulerv1.GetMobilityRecordRequest,
) (*schedulerv1.GetMobilityRecordResponse, error) {
	sandboxID := strings.TrimSpace(req.GetSandboxId())
	if sandboxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	record, found, err := s.mobility.Get(ctx, sandboxID)
	if err != nil {
		s.logger.Warn("scheduler mobility get failed",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "mobility store unavailable")
	}
	if !found {
		return &schedulerv1.GetMobilityRecordResponse{Found: false}, nil
	}
	return &schedulerv1.GetMobilityRecordResponse{
		Record: mobilityRecordToProto(record),
		Found:  true,
	}, nil
}

func (s *Service) ListMobilityRecords(
	ctx context.Context,
	req *schedulerv1.ListMobilityRecordsRequest,
) (*schedulerv1.ListMobilityRecordsResponse, error) {
	nodeID := strings.TrimSpace(req.GetOriginNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "origin_node_id is required")
	}
	records, err := s.mobility.ListByOrigin(ctx, nodeID)
	if err != nil {
		s.logger.Warn("scheduler mobility list failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "mobility store unavailable")
	}
	out := make([]*schedulerv1.MobilityRecord, 0, len(records))
	for _, record := range records {
		out = append(out, mobilityRecordToProto(record))
	}
	return &schedulerv1.ListMobilityRecordsResponse{Records: out}, nil
}

func (s *Service) RemoveMobilityRecord(
	ctx context.Context,
	req *schedulerv1.RemoveMobilityRecordRequest,
) (*schedulerv1.RemoveMobilityRecordResponse, error) {
	sandboxID := strings.TrimSpace(req.GetSandboxId())
	if sandboxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	if err := s.mobility.Remove(ctx, sandboxID); err != nil {
		s.logger.Warn("scheduler mobility remove failed",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "mobility store unavailable")
	}
	return &schedulerv1.RemoveMobilityRecordResponse{}, nil
}

func mobilityRecordFromProto(record *schedulerv1.MobilityRecord) (MobilityRecord, error) {
	if record == nil {
		return MobilityRecord{}, errMobilityRecordRequired
	}
	state, err := mobilityStateFromProto(record.GetState())
	if err != nil {
		return MobilityRecord{}, err
	}
	// Stored verbatim, but it has to be JSON: a record whose fingerprint
	// cannot be parsed by the node that eventually reads it is worse than one
	// that was refused at the door.
	fingerprint := strings.TrimSpace(record.GetFingerprintJson())
	var raw json.RawMessage
	if fingerprint != "" {
		if !json.Valid([]byte(fingerprint)) {
			return MobilityRecord{}, errMobilityFingerprintInvalid
		}
		raw = json.RawMessage(fingerprint)
	}

	out := MobilityRecord{
		SandboxID:     strings.TrimSpace(record.GetSandboxId()),
		OriginNodeID:  strings.TrimSpace(record.GetOriginNodeId()),
		Generation:    strings.TrimSpace(record.GetGeneration()),
		Fingerprint:   raw,
		ArtifactReach: strings.TrimSpace(record.GetArtifactReach()),
		CPUCount:      record.GetCpuCount(),
		MemoryMiB:     record.GetMemoryMib(),
		SnapshotID:    strings.TrimSpace(record.GetSnapshotId()),
		PausedAtMs:    record.GetPausedAtUnixMs(),
		State:         state,
		HolderNodeID:  strings.TrimSpace(record.GetHolderNodeId()),
		StateAtMs:     record.GetStateAtUnixMs(),
	}
	if err := out.valid(); err != nil {
		return MobilityRecord{}, err
	}
	// A claim or an evacuation that does not say who holds it is unusable:
	// the origin's fence has nothing to report and a second destination
	// cannot tell it is racing.
	if out.State != MobilityParked && out.HolderNodeID == "" {
		return MobilityRecord{}, errMobilityHolderRequired
	}
	return out, nil
}

func mobilityRecordToProto(record MobilityRecord) *schedulerv1.MobilityRecord {
	return &schedulerv1.MobilityRecord{
		SandboxId:       record.SandboxID,
		OriginNodeId:    record.OriginNodeID,
		Generation:      record.Generation,
		FingerprintJson: string(record.Fingerprint),
		ArtifactReach:   record.ArtifactReach,
		CpuCount:        record.CPUCount,
		MemoryMib:       record.MemoryMiB,
		SnapshotId:      record.SnapshotID,
		PausedAtUnixMs:  record.PausedAtMs,
		State:           mobilityStateToProto(record.State),
		HolderNodeId:    record.HolderNodeID,
		StateAtUnixMs:   record.StateAtMs,
	}
}

func mobilityStateFromProto(state schedulerv1.MobilityState) (string, error) {
	switch state {
	case schedulerv1.MobilityState_MOBILITY_STATE_PARKED:
		return MobilityParked, nil
	case schedulerv1.MobilityState_MOBILITY_STATE_CLAIMED:
		return MobilityClaimed, nil
	case schedulerv1.MobilityState_MOBILITY_STATE_EVACUATED:
		return MobilityEvacuated, nil
	default:
		// Unspecified is what a newer node's unknown state arrives as, and
		// what an older node that never set the field sends. Both are refused:
		// guessing "parked" would advertise a sandbox as available while it is
		// mid-handover.
		return "", errMobilityStateUnspecified
	}
}

func mobilityStateToProto(state string) schedulerv1.MobilityState {
	switch state {
	case MobilityParked:
		return schedulerv1.MobilityState_MOBILITY_STATE_PARKED
	case MobilityClaimed:
		return schedulerv1.MobilityState_MOBILITY_STATE_CLAIMED
	case MobilityEvacuated:
		return schedulerv1.MobilityState_MOBILITY_STATE_EVACUATED
	default:
		return schedulerv1.MobilityState_MOBILITY_STATE_UNSPECIFIED
	}
}
