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
