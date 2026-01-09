package nntpcli

import (
	"bytes"
	"errors"
	"testing"

	"go.uber.org/mock/gomock"
)

func TestPipelineRequest_BasicFields(t *testing.T) {
	buf := &bytes.Buffer{}
	req := PipelineRequest{
		MessageID: "test@example.com",
		Writer:    buf,
		Discard:   100,
	}

	if req.MessageID != "test@example.com" {
		t.Errorf("MessageID = %q, want %q", req.MessageID, "test@example.com")
	}
	if req.Writer != buf {
		t.Error("Writer not set correctly")
	}
	if req.Discard != 100 {
		t.Errorf("Discard = %d, want %d", req.Discard, 100)
	}
}

func TestPipelineResult_BasicFields(t *testing.T) {
	testErr := errors.New("test error")
	result := PipelineResult{
		MessageID:    "test@example.com",
		BytesWritten: 1024,
		Error:        testErr,
	}

	if result.MessageID != "test@example.com" {
		t.Errorf("MessageID = %q, want %q", result.MessageID, "test@example.com")
	}
	if result.BytesWritten != 1024 {
		t.Errorf("BytesWritten = %d, want %d", result.BytesWritten, 1024)
	}
	if result.Error != testErr {
		t.Errorf("Error = %v, want %v", result.Error, testErr)
	}
}

func TestPipelineResult_NoError(t *testing.T) {
	result := PipelineResult{
		MessageID:    "test@example.com",
		BytesWritten: 2048,
		Error:        nil,
	}

	if result.Error != nil {
		t.Errorf("Error = %v, want nil", result.Error)
	}
}

func TestBodyPipelined_EmptyRequests_ViaInterface(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := NewMockConnection(ctrl)
	mockConn.EXPECT().BodyPipelined([]PipelineRequest{}).Return([]PipelineResult{})

	results := mockConn.BodyPipelined([]PipelineRequest{})

	if len(results) != 0 {
		t.Errorf("len(results) = %d, want 0", len(results))
	}
}

func TestBodyPipelined_SingleRequest_ViaInterface(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	buf := &bytes.Buffer{}
	requests := []PipelineRequest{
		{MessageID: "single@test.com", Writer: buf, Discard: 0},
	}

	expectedResults := []PipelineResult{
		{MessageID: "single@test.com", BytesWritten: 1024, Error: nil},
	}

	mockConn := NewMockConnection(ctrl)
	mockConn.EXPECT().BodyPipelined(requests).Return(expectedResults)

	results := mockConn.BodyPipelined(requests)

	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}

	if results[0].MessageID != "single@test.com" {
		t.Errorf("MessageID = %q, want %q", results[0].MessageID, "single@test.com")
	}

	if results[0].BytesWritten != 1024 {
		t.Errorf("BytesWritten = %d, want %d", results[0].BytesWritten, 1024)
	}

	if results[0].Error != nil {
		t.Errorf("Error = %v, want nil", results[0].Error)
	}
}

func TestBodyPipelined_MultipleRequests_ViaInterface(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	buf3 := &bytes.Buffer{}

	requests := []PipelineRequest{
		{MessageID: "msg1@test.com", Writer: buf1, Discard: 0},
		{MessageID: "msg2@test.com", Writer: buf2, Discard: 0},
		{MessageID: "msg3@test.com", Writer: buf3, Discard: 0},
	}

	expectedResults := []PipelineResult{
		{MessageID: "msg1@test.com", BytesWritten: 1024, Error: nil},
		{MessageID: "msg2@test.com", BytesWritten: 2048, Error: nil},
		{MessageID: "msg3@test.com", BytesWritten: 512, Error: nil},
	}

	mockConn := NewMockConnection(ctrl)
	mockConn.EXPECT().BodyPipelined(requests).Return(expectedResults)

	results := mockConn.BodyPipelined(requests)

	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}

	for i, expected := range expectedResults {
		if results[i].MessageID != expected.MessageID {
			t.Errorf("results[%d].MessageID = %q, want %q", i, results[i].MessageID, expected.MessageID)
		}
		if results[i].BytesWritten != expected.BytesWritten {
			t.Errorf("results[%d].BytesWritten = %d, want %d", i, results[i].BytesWritten, expected.BytesWritten)
		}
		if results[i].Error != expected.Error {
			t.Errorf("results[%d].Error = %v, want %v", i, results[i].Error, expected.Error)
		}
	}
}

func TestBodyPipelined_PartialErrors_ViaInterface(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testErr := errors.New("article not found")

	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	requests := []PipelineRequest{
		{MessageID: "msg1@test.com", Writer: buf1, Discard: 0},
		{MessageID: "notfound@test.com", Writer: buf2, Discard: 0},
	}

	expectedResults := []PipelineResult{
		{MessageID: "msg1@test.com", BytesWritten: 1024, Error: nil},
		{MessageID: "notfound@test.com", BytesWritten: 0, Error: testErr},
	}

	mockConn := NewMockConnection(ctrl)
	mockConn.EXPECT().BodyPipelined(requests).Return(expectedResults)

	results := mockConn.BodyPipelined(requests)

	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}

	// First request should succeed
	if results[0].Error != nil {
		t.Errorf("results[0].Error = %v, want nil", results[0].Error)
	}
	if results[0].BytesWritten != 1024 {
		t.Errorf("results[0].BytesWritten = %d, want 1024", results[0].BytesWritten)
	}

	// Second request should fail
	if results[1].Error != testErr {
		t.Errorf("results[1].Error = %v, want %v", results[1].Error, testErr)
	}
	if results[1].BytesWritten != 0 {
		t.Errorf("results[1].BytesWritten = %d, want 0", results[1].BytesWritten)
	}
}

func TestTestPipelineSupport_ViaInterface(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMsgID := "test@example.com"

	mockConn := NewMockConnection(ctrl)
	mockConn.EXPECT().TestPipelineSupport(testMsgID).Return(true, 5, nil)

	supported, depth, err := mockConn.TestPipelineSupport(testMsgID)

	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if !supported {
		t.Error("supported = false, want true")
	}
	if depth != 5 {
		t.Errorf("depth = %d, want 5", depth)
	}
}

func TestTestPipelineSupport_NotSupported_ViaInterface(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMsgID := "test@example.com"
	testErr := errors.New("pipelining not supported")

	mockConn := NewMockConnection(ctrl)
	mockConn.EXPECT().TestPipelineSupport(testMsgID).Return(false, 0, testErr)

	supported, depth, err := mockConn.TestPipelineSupport(testMsgID)

	if err != testErr {
		t.Errorf("err = %v, want %v", err, testErr)
	}
	if supported {
		t.Error("supported = true, want false")
	}
	if depth != 0 {
		t.Errorf("depth = %d, want 0", depth)
	}
}
