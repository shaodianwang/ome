package query

import "testing"

func TestRevisionHashFromControllerRevisionName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no separator", "abcd1234", ""},
		{"single separator", "isvc-hash", "hash"},
		{"isvc-component-hash", "llama-engine-abcd1234", "abcd1234"},
		{"trailing dash returns empty suffix", "llama-engine-", ""},
		{"isvc name with dashes", "llama-70b-instruct-engine-abcd1234", "abcd1234"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RevisionHashFromControllerRevisionName(tc.in); got != tc.want {
				t.Errorf("RevisionHashFromControllerRevisionName(%q): got %q want %q", tc.in, got, tc.want)
			}
		})
	}
}
