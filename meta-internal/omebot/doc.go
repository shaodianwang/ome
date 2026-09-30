//go:build meta_internal

// Package omebot is the deterministic core of the OME Slack bot: an HTTP
// service that answers structured commands by running kubectl-ome against
// every cluster in its registry, reports which clusters run OME, and tells a
// question apart from a command so the relay can hand it to the answer
// worker.
//
// It deliberately owns no domain logic. Every answer about an OME object is
// produced by kubectl-ome; the service only routes, authorizes, bounds, and
// renders. Slack itself is never touched here: a relay in front of this
// service receives the slash command or the mention and posts the reply, so
// the service needs no Slack credential.
package omebot
