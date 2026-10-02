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
//
// Read verbs run anywhere in the registry. kubectl-ome's guarded verbs, the
// ones that change a cluster, run only when mutations are switched on, the
// named cluster is marked writable, the relay vouched for the actor
// (Actor.CanMutate) and the caller typed --yes; each missing condition is
// refused with its own error class, and the reply of one that ran carries
// Mutation so the relay can post it in the open.
package omebot
