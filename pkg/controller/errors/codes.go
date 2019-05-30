package errors

type ErrorCode = string

const (
	IllegalStateError        ErrorCode = "IllegalStateError"
	CausedByError            ErrorCode = "CausedByError"
	BadJobSpecificationError ErrorCode = "BadJobSpecificationError"
	ReconciliationNeeded     ErrorCode = "ReconciliationNeeded"
)
