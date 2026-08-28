package scheduler

import "errors"

var (
	errMobilityRecordRequired     = errors.New("mobility record is required")
	errMobilityStateUnspecified   = errors.New("mobility record state is unspecified")
	errMobilityHolderRequired     = errors.New("a claimed or evacuated record must name its holder")
	errMobilityFingerprintInvalid = errors.New("mobility fingerprint must be valid json")
)
