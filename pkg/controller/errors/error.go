package errors

import (
	"fmt"
)

type ErrorMessage = string

type FlinkOperatorError struct {
	Code    ErrorCode
	Message ErrorMessage
}

func (w *FlinkOperatorError) Error() string {
	return fmt.Sprintf("ErrorCode: [%v] Reason: [%v]", w.Code, w.Message)
}

type FlinkOperatorErrorWithCause struct {
	*FlinkOperatorError
	cause error
}

func (w *FlinkOperatorErrorWithCause) Error() string {
	return fmt.Sprintf("%v. Caused By [%v]", w.FlinkOperatorError.Error(), w.cause)
}

func (w *FlinkOperatorErrorWithCause) Cause() error {
	return w.cause
}

func errorf(c ErrorCode, msgFmt string, args ...interface{}) *FlinkOperatorError {
	return &FlinkOperatorError{
		Code:    c,
		Message: fmt.Sprintf(msgFmt, args...),
	}
}

func Errorf(c ErrorCode, msgFmt string, args ...interface{}) error {
	return errorf(c, msgFmt, args...)
}

func WrapErrorf(c ErrorCode, cause error, msgFmt string, args ...interface{}) error {
	return &FlinkOperatorErrorWithCause{
		FlinkOperatorError: errorf(c, msgFmt, args...),
		cause:              cause,
	}
}

func IsReconciliationNeeded(err error) bool {
	if fErr, ok := err.(*FlinkOperatorError); ok {
		if fErr.Code == ReconciliationNeeded {
			return true
		}
	}
	return false
}
