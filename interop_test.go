//go:generate mockgen -source=interop.go -destination=mocks/mock_interop.go -package=mocks

package interop

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/ezotrank/interop/mocks"
)

func TestInterop_Start_RuleWithDelay(t *testing.T) {
	ctrl := gomock.NewController(t)

	reader := mocks.NewMockireader(ctrl)
	reader.EXPECT().
		FetchMessage(gomock.Any()).
		Return(kafka.Message{
			Topic: "topic",
			Key:   []byte("key"),
			Value: []byte("value"),
		}, nil)
	reader.EXPECT().Close().Return(nil)

	writer := mocks.NewMockiwriter(ctrl)
	writer.EXPECT().Close().Return(nil)

	inrp := &Interop{
		rules: []Rule{
			{
				Topic: "topic",
				Handler: func(ctx context.Context, msg kafka.Message) error {
					return fmt.Errorf("error")
				},
				Attempts:   3,
				Ordered:    true,
				RetryDelay: 100 * time.Millisecond,
			},
		},
		reader: reader,
		writer: writer,
	}

	ts := time.Now()
	require.Error(t, inrp.Start(context.Background()))
	// time of execution should be more that two times by 100ms.
	require.True(t, time.Since(ts) >= 200*time.Millisecond)
}

//nolint:funlen
func TestInterop_Start(t *testing.T) {
	type fields struct {
		rules  []Rule
		reader *mocks.Mockireader
		writer *mocks.Mockiwriter
	}
	var hexec int // number of handler executions
	tests := []struct {
		name     string
		rules    []Rule
		prepare  func(f *fields)
		wantexec int
		wantErr  bool
	}{
		{
			name: "success flow",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return nil
					},
					Attempts: 1,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				f.reader.EXPECT().
					CommitMessages(gomock.Any(), kafka.Message{
						Topic:   "topic",
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: nil,
					}).
					Return(nil).
					Times(1)
			},
			wantexec: 1,
			wantErr:  false,
		},
		{
			name: "attempts number is zero",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						return nil
					},
					Attempts: 0,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
				)
			},
			wantexec: 0,
			wantErr:  true,
		},
		{
			name: "fetch message error",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return nil
					},
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic",
						Key:   []byte("key"),
						Value: []byte("value"),
					}, fmt.Errorf("error"))
			},
			wantexec: 0,
			wantErr:  true,
		},
		{
			name: "message with unknown topic",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return nil
					},
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic2",
						Key:   []byte("key"),
						Value: []byte("value"),
					}, nil)
			},
			wantexec: 0,
			wantErr:  true,
		},
		{
			name: "handle return error without retry policy",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					Attempts: 1,
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic",
						Key:   []byte("key"),
						Value: []byte("value"),
					}, nil)
			},
			wantexec: 1,
			wantErr:  true,
		},
		{
			name: "commit message return error",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return nil
					},
					Attempts: 1,
				},
			},
			prepare: func(f *fields) {
				f.reader.EXPECT().
					FetchMessage(gomock.Any()).
					Return(kafka.Message{
						Topic: "topic",
						Key:   []byte("key"),
						Value: []byte("value"),
					}, nil)
				f.reader.EXPECT().
					CommitMessages(gomock.Any(), kafka.Message{
						Topic:   "topic",
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: nil,
					}).
					Return(fmt.Errorf("error"))
			},
			wantexec: 1,
			wantErr:  true,
		},
		{
			name: "handle return error with retry policy",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					Attempts: 3,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}, nil),
				)
				gomock.InOrder(
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}).
						Return(nil),
				)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
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
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					Attempts: 1,
					DLQ:      "dlq",
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				f.writer.EXPECT().
					WriteMessages(gomock.Any(), kafka.Message{
						Topic:   "dlq",
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: nil,
					}).
					Return(nil)
				f.reader.EXPECT().
					CommitMessages(gomock.Any(), kafka.Message{
						Topic:   "topic",
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: nil,
					}).
					Return(nil)
			},
			wantexec: 1,
			wantErr:  false,
		},
		{
			name: "handle return error only first retry policy is set",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						if hexec == 2 {
							return nil
						}
						return fmt.Errorf("error")
					},
					Attempts: 2,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
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
						Topic: "topic",
						Key:   []byte("key"),
						Value: []byte("value"),
						Headers: []kafka.Header{
							{Key: AttemptsHeader, Value: []byte("1")},
						},
					}).
					Return(nil)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
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
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					Attempts: 1,
					DLQ:      "retry",
				},
				{
					Topic: "retry",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					Attempts: 3,
					DLQ:      "dlq",
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic:   "retry",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "retry",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "retry",
							Key:   []byte("key"),
							Value: []byte("value"),
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
							Topic:   "retry",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("2")},
							},
						}).
						Return(nil),
					f.writer.EXPECT().
						WriteMessages(gomock.Any(), kafka.Message{
							Topic:   "dlq",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: []kafka.Header{},
						}).
						Return(nil),
				)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "retry",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Key:   []byte("key"),
							Value: []byte("value"),
							Headers: []kafka.Header{
								{Key: AttemptsHeader, Value: []byte("1")},
							},
						}).
						Return(nil),
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic: "retry",
							Key:   []byte("key"),
							Value: []byte("value"),
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
		{
			name: "ordered flow with retry and failed handler",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					Attempts: 3,
					Ordered:  true,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
				)
			},
			wantexec: 3,
			wantErr:  true,
		},
		{
			name: "ordered flow with a retry and handler with success in second time",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						if hexec == 2 {
							return nil
						}
						return fmt.Errorf("error")
					},
					Attempts: 2,
					Ordered:  true,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
				)
			},
			wantexec: 2,
			wantErr:  false,
		},
		{
			name: "ordered flow with retry and sending to DQL",
			rules: []Rule{
				{
					Topic: "topic",
					Handler: func(ctx context.Context, msg kafka.Message) error {
						hexec++
						return fmt.Errorf("error")
					},
					DLQ:      "dlq",
					Attempts: 2,
					Ordered:  true,
				},
			},
			prepare: func(f *fields) {
				gomock.InOrder(
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						Return(kafka.Message{
							Topic: "topic",
							Key:   []byte("key"),
							Value: []byte("value"),
						}, nil),
					f.reader.EXPECT().
						FetchMessage(gomock.Any()).
						DoAndReturn(func(_ context.Context) (kafka.Message, error) {
							return kafka.Message{}, io.EOF
						}),
				)
				f.writer.EXPECT().
					WriteMessages(gomock.Any(), kafka.Message{
						Topic:   "dlq",
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: nil,
					}).
					Return(nil)
				gomock.InOrder(
					f.reader.EXPECT().
						CommitMessages(gomock.Any(), kafka.Message{
							Topic:   "topic",
							Key:     []byte("key"),
							Value:   []byte("value"),
							Headers: nil,
						}).
						Return(nil),
				)
			},
			wantexec: 2,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hexec = 0
			ctrl := gomock.NewController(t)

			f := fields{
				rules:  tt.rules,
				reader: mocks.NewMockireader(ctrl),
				writer: mocks.NewMockiwriter(ctrl),
			}

			if tt.prepare != nil {
				tt.prepare(&f)
			}

			f.reader.EXPECT().Close().Return(nil)
			f.writer.EXPECT().Close().Return(nil)

			i := &Interop{
				rules:  f.rules,
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

func Test_topics(t *testing.T) {
	type fields struct {
		rules []Rule
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "return listen topics",
			fields: fields{
				rules: []Rule{
					{Topic: "topic-1"},
					{Topic: "topic-2"},
					{Topic: "topic-3"},
				},
			},
			want: []string{"topic-1", "topic-2", "topic-3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ElementsMatch(t, tt.want, topics(tt.fields.rules...))
		})
	}
}

func Test_dupls(t *testing.T) {
	type args struct {
		rules []Rule
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "return duplicates topics",
			args: args{
				rules: []Rule{
					{Topic: "topic-1"},
					{Topic: "topic-1"},
					{Topic: "topic-2"},
					{Topic: "topic-3"},
				},
			},
			want: []string{
				"topic-1",
			},
		},
		{
			name: "topics without duplicates",
			args: args{
				rules: []Rule{
					{Topic: "topic-1"},
					{Topic: "topic-2"},
					{Topic: "topic-3"},
				},
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dupls(tt.args.rules); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dupls() = %v, want %v", got, tt.want)
			}
		})
	}
}
