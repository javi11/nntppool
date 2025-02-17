// Code generated by MockGen. DO NOT EDIT.
// Source: ./pooled_connection.go
//
// Generated by this command:
//
//	mockgen -source=./pooled_connection.go -destination=./pooled_connection_mock.go -package=nntppool PooledConnection
//

// Package nntppool is a generated GoMock package.
package nntppool

import (
	reflect "reflect"
	time "time"

	nntpcli "github.com/javi11/nntpcli"
	gomock "go.uber.org/mock/gomock"
)

// MockPooledConnection is a mock of PooledConnection interface.
type MockPooledConnection struct {
	ctrl     *gomock.Controller
	recorder *MockPooledConnectionMockRecorder
	isgomock struct{}
}

// MockPooledConnectionMockRecorder is the mock recorder for MockPooledConnection.
type MockPooledConnectionMockRecorder struct {
	mock *MockPooledConnection
}

// NewMockPooledConnection creates a new mock instance.
func NewMockPooledConnection(ctrl *gomock.Controller) *MockPooledConnection {
	mock := &MockPooledConnection{ctrl: ctrl}
	mock.recorder = &MockPooledConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPooledConnection) EXPECT() *MockPooledConnectionMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockPooledConnection) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockPooledConnectionMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockPooledConnection)(nil).Close))
}

// Connection mocks base method.
func (m *MockPooledConnection) Connection() nntpcli.Connection {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Connection")
	ret0, _ := ret[0].(nntpcli.Connection)
	return ret0
}

// Connection indicates an expected call of Connection.
func (mr *MockPooledConnectionMockRecorder) Connection() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Connection", reflect.TypeOf((*MockPooledConnection)(nil).Connection))
}

// CreatedAt mocks base method.
func (m *MockPooledConnection) CreatedAt() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreatedAt")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// CreatedAt indicates an expected call of CreatedAt.
func (mr *MockPooledConnectionMockRecorder) CreatedAt() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatedAt", reflect.TypeOf((*MockPooledConnection)(nil).CreatedAt))
}

// Free mocks base method.
func (m *MockPooledConnection) Free() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Free")
}

// Free indicates an expected call of Free.
func (mr *MockPooledConnectionMockRecorder) Free() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Free", reflect.TypeOf((*MockPooledConnection)(nil).Free))
}

// Provider mocks base method.
func (m *MockPooledConnection) Provider() ConnectionProviderInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Provider")
	ret0, _ := ret[0].(ConnectionProviderInfo)
	return ret0
}

// Provider indicates an expected call of Provider.
func (mr *MockPooledConnectionMockRecorder) Provider() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Provider", reflect.TypeOf((*MockPooledConnection)(nil).Provider))
}
