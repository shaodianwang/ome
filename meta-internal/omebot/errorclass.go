//go:build meta_internal

package omebot

import "fmt"

// Error classes let a caller branch on why a request was refused without
// parsing the human-readable message.
const (
	ErrNotAVerb             = "not_a_verb"
	ErrForbiddenFlag        = "forbidden_flag"
	ErrUnknownCluster       = "unknown_cluster"
	ErrNeedsCluster         = "needs_cluster"
	ErrMalformed            = "malformed"
	ErrBusy                 = "busy"
	ErrUnknownTool          = "unknown_tool"
	ErrSubcommandNotAllowed = "subcommand_not_allowed"
	ErrBadArgs              = "bad_args"
	ErrTimeout              = "timeout"
	ErrNotConfigured        = "not_configured"
	// ErrQuestion marks text meant for the answer worker, not a verb; the
	// reply carries the question for the relay to hand on.
	ErrQuestion = "question"
	// A guarded verb changes a cluster. It runs only when mutations are
	// switched on, the cluster is marked writable, the relay has cleared the
	// actor, and the request carries --yes; each missing condition has its
	// own class so the relay can say which.
	ErrMutationsOff       = "mutations_off"
	ErrClusterNotWritable = "cluster_not_writable"
	ErrActorNotAllowed    = "actor_not_allowed"
	ErrNeedsConfirmation  = "needs_confirmation"
)

// ParseError is a refusal with a machine-readable class.
type ParseError struct {
	Class string
	Msg   string
}

func (e *ParseError) Error() string { return e.Msg }

func refuse(class, format string, args ...any) *ParseError {
	return &ParseError{Class: class, Msg: fmt.Sprintf(format, args...)}
}
