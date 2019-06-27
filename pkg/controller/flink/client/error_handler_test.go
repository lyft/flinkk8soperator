package client

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func getTestRetryer() RetryHandler {
	return NewRetryHandler(10*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
}

func TestGetError(t *testing.T) {
	testErr := errors.New("Service unavailable")
	ferr := GetError(testErr, "GetTest", "500", true, false, defaultRetries)
	assert.Equal(t, "GetTest call failed with status 500 and message []: Service unavailable", ferr.Error())

	//nil error
	ferrNil := GetError(nil, "GetTest", "500", true, false, defaultRetries)
	assert.Equal(t, "GetTest call failed with status 500 and message []", ferrNil.Error())

	testWrappedErr := errors.Wrap(testErr, "Wrapped errors")
	ferrWrapped := GetError(testWrappedErr, "GetTestWrapped", "400", true, false, defaultRetries)
	assert.Equal(t, "GetTestWrapped call failed with status 400 and message []: Wrapped errors: Service unavailable", ferrWrapped.Error())

	testMessageErr := errors.New("Test Error")
	ferrMessage := GetError(testMessageErr, "GetTest", "500", true, false, defaultRetries, "message1", "message2")
	assert.Equal(t, "GetTest call failed with status 500 and message [message1 message2]: Test Error", ferrMessage.Error())
}

func TestErrors(t *testing.T) {
	retryableError := errors.New("GetClusterOverview500")
	ferr := GetError(retryableError, "GetTest", "500", true, false, defaultRetries)
	retryer := getTestRetryer()
	assert.True(t, retryer.IsErrorRetryable(ferr))
	assert.False(t, retryer.IsErrorFailFast(ferr))

	failFastError := errors.New("SubmitJob400BadRequest")
	ferr = GetError(failFastError, "GetTest", "500", false, true, defaultRetries)
	assert.False(t, retryer.IsErrorRetryable(ferr))
	assert.True(t, retryer.IsErrorFailFast(ferr))

	otherError := errors.New("CancelJob")
	assert.False(t, retryer.IsErrorRetryable(otherError))
	assert.False(t, retryer.IsErrorFailFast(otherError))
}

func TestRetryHandler_BackOff(t *testing.T) {
	retryHandler := getTestRetryer()
	assert.True(t, retryHandler.GetRetryDelay(0) <= 50*time.Millisecond)
	assert.True(t, retryHandler.GetRetryDelay(1) <= 50*time.Millisecond)
}

func TestRetryHandler_IsRetryRemaining(t *testing.T) {
	retryableError := errors.New("GetClusterOverview500")
	ferr := GetError(retryableError, "GetTest", "500", true, false, defaultRetries)
	retryer := getTestRetryer()
	assert.True(t, retryer.IsRetryRemaining(ferr, 2))
	assert.False(t, retryer.IsRetryRemaining(ferr, 22))
}
