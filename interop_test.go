//go:generate mockgen -source=interop.go -destination=mocks/mock_interop.go -package=mocks

package interop

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/ezotrank/interop/mocks"
)

//nolint:funlen
func TestInterop_Start(t *testing.T) {
	type fields struct {
		flow   Flow
		reader *mocks.Mockireader
		writer *mocks.Mockiwriter
	}
	var hexec int // number of handler executions
	tests := []struct {
		name     string
		flow     Flow
		prepare  func(f *fields)
		wantexec int
		wantErr  bool
	}{
		{
			name: "success flow",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return nil
						},
					},
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				f.reader.EXPECT().
					CommitMessages(gomock.Any(), kafka.Message{
						Topic:   "topic1",
						Headers: nil,
					}).
					Return(nil).
					Times(1)
			},
			wantexec: 1,
			wantErr:  false,
		},
		{
			name: "fetch message error",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return nil
						},
					},
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic1",
					}, fmt.Errorf("error"))
			},
			wantexec: 0,
			wantErr:  true,
		},
		{
			name: "message with unknown topic",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return nil
						},
					},
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic2",
					}, nil)
			},
			wantexec: 0,
			wantErr:  true,
		},
		{
			name: "handle return error without retry policy",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return fmt.Errorf("error")
						},
						Attempts: 1,
					},
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic1",
					}, nil)
			},
			wantexec: 1,
			wantErr:  true,
		},
		{
			name: "commit message return error",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return nil
						},
						Attempts: 1,
					},
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic1",
					}, nil)
				f.reader.EXPECT().
					CommitMessages(gomock.Any(), kafka.Message{
						Topic:   "topic1",
						Headers: nil,
					}).
					Return(fmt.Errorf("error"))
			},
			wantexec: 1,
			wantErr:  true,
		},
		{
			name: "handle return error with retry policy",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return fmt.Errorf("error")
						},
						Attempts: 3,
					},
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}, nil),
				)
				gomock.InOrder(
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}).
						Return(nil),
				)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic1",
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
				)
			},
			wantexec: 3,
			wantErr:  true,
		},
		{
			name: "handle return error with retry policy and DLQ",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return fmt.Errorf("error")
						},
						DLQ: "dlq",
					},
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				f.writer.EXPECT().
					WriteMessages(gomock.Any(), kafka.Message{
						Topic: "dlq",
						Headers: []kafka.Header{
							{Key: AttemptsHeader, Value: []byte("0")},
						},
					}).
					Return(nil)
				f.reader.EXPECT().
					CommitMessages(gomock.Any(), kafka.Message{
						Topic:   "topic1",
						Headers: nil,
					}).
					Return(nil)
			},
			wantexec: 1,
			wantErr:  false,
		},
		{
			name: "handle return error only first retry policy is set",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							if getAttempts(msg.Headers) == 1 {
								return nil
							}
							return fmt.Errorf("error")
						},
						Attempts: 2,
					},
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				f.writer.EXPECT().
					WriteMessages(gomock.Any(), kafka.Message{
						Topic: "topic1",
						Headers: []kafka.Header{
							{Key: AttemptsHeader, Value: []byte("1")},
						},
					}).
					Return(nil)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic1",
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "topic1",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
				)
			},
			wantexec: 2,
			wantErr:  false,
		},
		{
			name: "handle return error dlq is set with retries",
			flow: Flow{
				Rules: map[string]Rule{
					"topic1": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return fmt.Errorf("error")
						},
						Attempts: 1,
						DLQ:      "retry",
					},
					"retry": {
						Handler: func(ctx context.Context, msg kafka.Message) error {
							hexec++
							return fmt.Errorf("error")
						},
						Attempts: 3,
						DLQ:      "dlq",
					},
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic1",
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("0")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				gomock.InOrder(
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("0")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "dlq",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("0")},
							},
						}).
						Return(nil),
				)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic1",
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("0")},
							},
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}).
						Return(nil),
				)
			},
			wantexec: 4,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hexec = 0
			ctrl := gomock.NewController(t)

			f := fields{
				flow:   tt.flow,
				reader: mocks.NewMockireader(ctrl),
				writer: mocks.NewMockiwriter(ctrl),
			}

			if tt.prepare != nil {
				tt.prepare(&f)
			}

			f.reader.EXPECT().Close().Return(nil)
			f.writer.EXPECT().Close().Return(nil)

			i := &Interop{
				flow:   f.flow,
				reader: f.reader,
				writer: f.writer,
			}

			if err := i.Start(context.Background()); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
			require.Equal(t, tt.wantexec, hexec)
		})
	}
}
