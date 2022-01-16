package interop

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlow_listenTopics(t *testing.T) {
	type fields struct {
		Rules map[string]Rule
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "return listen topics",
			fields: fields{
				Rules: map[string]Rule{
					"topic-1": {},
					"topic-2": {},
					"topic-3": {},
				},
			},
			want: []string{"topic-1", "topic-2", "topic-3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Flow{
				Rules: tt.fields.Rules,
			}
			require.ElementsMatch(t, tt.want, f.listenTopics())
		})
	}
}
