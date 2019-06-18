package client

import "github.com/pkg/errors"

// appError codes
const (
	globalFailure      = "FAILED"
	jsonUnmarshalError = "JSONUNMARSHALERROR"
)

var retryableErrors = map[string]struct{}{
	"GetClusterOverview":   {},
	"GetJobs":              {},
	"CheckSavepointStatus": {},
}

// FlinkApplicationError implements the appError interface to make appError handling more structured
type FlinkApplicationError struct {
	appError  string
	method    string
	errorCode string
}

func (f *FlinkApplicationError) Error() string {
	return f.appError
}

func IsErrorRetryable(err string) bool {
	if _, ok := retryableErrors[err]; ok {
		return true
	}
	return false
}

func GetError(err error, method string, errorCode string) error {
	var f = new(FlinkApplicationError)
	if err == nil {
		err = errors.New(method + errorCode)
	} else {
		err = errors.Wrap(err, method+errorCode)
	}
	f.errorCode = errorCode
	f.appError = err.Error()
	f.method = method
	return f
}

func GetErrorKey(error error) string {
	flinkAppError, ok := error.(*FlinkApplicationError)
	if ok && flinkAppError != nil {
		return flinkAppError.method + flinkAppError.errorCode
	}
	return ""
}
