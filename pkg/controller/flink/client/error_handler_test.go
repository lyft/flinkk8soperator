package client

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func getTestRetryer() RetryHandler {
	return NewRetryHandler(2, 10)
}

func TestGetError(t *testing.T) {
	testErr := errors.New("Service unavailable")
	ferr := GetError(testErr, "GetTest", "500")
	assert.Equal(t, "GetTest call failed with status 500 and message []: Service unavailable", ferr.Error())

	//nil error
	ferrNil := GetError(nil, "GetTest", "500")
	assert.Equal(t, "GetTest call failed with status 500 and message []", ferrNil.Error())

	testWrappedErr := errors.Wrap(testErr, "Wrapped errors")
	ferrWrapped := GetError(testWrappedErr, "GetTestWrapped", "400")
	assert.Equal(t, "GetTestWrapped call failed with status 400 and message []: Wrapped errors: Service unavailable", ferrWrapped.Error())

	testMessageErr := errors.New("Test Error")
	ferrMessage := GetError(testMessageErr, "GetTest", "500", "message1", "message2")
	assert.Equal(t, "GetTest call failed with status 500 and message [message1 message2]: Test Error", ferrMessage.Error())
}

func TestGetErrorKey(t *testing.T) {
	testErr := errors.New("Service unavailable")
	ferr := GetError(testErr, "GetTest", "500")
	assert.NotEmpty(t, GetErrorKey(ferr))
	assert.Equal(t, GetErrorKey(ferr), "GetTest500")
}

func TestRetryHandler_IsErrorRetryable(t *testing.T) {
	errString := "GetClusterOverview500"
	retryer := getTestRetryer()
	assert.True(t, retryer.IsErrorRetryable(errString, 0))
	assert.False(t, retryer.IsErrorRetryable(errString, 3))

	errString = "NotRetryableError"
	assert.False(t, retryer.IsErrorRetryable(errString, 0))
}

func TestRetryHandler_BackOff(t *testing.T) {
	retryHandler := getTestRetryer()
	assert.Equal(t, 10*time.Millisecond, retryHandler.GetRetryDelay(0))
	assert.Equal(t, 20*time.Millisecond, retryHandler.GetRetryDelay(1))
}
