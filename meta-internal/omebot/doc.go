//go:build meta_internal

// Package omebot is the deterministic core of the OME Slack bot: an HTTP
// service that answers structured `/ome` commands by running kubectl-ome
// against every cluster in its registry, and reports which clusters run OME.
//
// It deliberately owns no domain logic. Every answer about an OME object is
// produced by kubectl-ome; the service only routes, authorizes, bounds, and
// renders. Slack itself is never touched here: a relay in front of this
// service receives the slash command and posts the reply, so the service
// needs no Slack credential.
package omebot
