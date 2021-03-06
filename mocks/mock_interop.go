// Code generated by MockGen. DO NOT EDIT.
// Source: interop.go

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	kafka "github.com/segmentio/kafka-go"
)

// Mockireader is a mock of ireader interface.
type Mockireader struct {
	ctrl     *gomock.Controller
	recorder *MockireaderMockRecorder
}

// MockireaderMockRecorder is the mock recorder for Mockireader.
type MockireaderMockRecorder struct {
	mock *Mockireader
}

// NewMockireader creates a new mock instance.
func NewMockireader(ctrl *gomock.Controller) *Mockireader {
	mock := &Mockireader{ctrl: ctrl}
	mock.recorder = &MockireaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockireader) EXPECT() *MockireaderMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *Mockireader) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockireaderMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*Mockireader)(nil).Close))
}

// CommitMessages mocks base method.
func (m *Mockireader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx}
	for _, a := range messages {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CommitMessages", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// CommitMessages indicates an expected call of CommitMessages.
func (mr *MockireaderMockRecorder) CommitMessages(ctx interface{}, messages ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx}, messages...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommitMessages", reflect.TypeOf((*Mockireader)(nil).CommitMessages), varargs...)
}

// FetchMessage mocks base method.
func (m *Mockireader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchMessage", ctx)
	ret0, _ := ret[0].(kafka.Message)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchMessage indicates an expected call of FetchMessage.
func (mr *MockireaderMockRecorder) FetchMessage(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchMessage", reflect.TypeOf((*Mockireader)(nil).FetchMessage), ctx)
}

// Mockiwriter is a mock of iwriter interface.
type Mockiwriter struct {
	ctrl     *gomock.Controller
	recorder *MockiwriterMockRecorder
}

// MockiwriterMockRecorder is the mock recorder for Mockiwriter.
type MockiwriterMockRecorder struct {
	mock *Mockiwriter
}

// NewMockiwriter creates a new mock instance.
func NewMockiwriter(ctrl *gomock.Controller) *Mockiwriter {
	mock := &Mockiwriter{ctrl: ctrl}
	mock.recorder = &MockiwriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockiwriter) EXPECT() *MockiwriterMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *Mockiwriter) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockiwriterMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*Mockiwriter)(nil).Close))
}

// WriteMessages mocks base method.
func (m *Mockiwriter) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx}
	for _, a := range messages {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WriteMessages", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// WriteMessages indicates an expected call of WriteMessages.
func (mr *MockiwriterMockRecorder) WriteMessages(ctx interface{}, messages ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx}, messages...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteMessages", reflect.TypeOf((*Mockiwriter)(nil).WriteMessages), varargs...)
}
