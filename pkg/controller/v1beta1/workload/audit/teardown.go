package audit

// OutcomeOwnerTornDown is the Outcome stamped when teardown closes a
// dangling in-flight (Phase=Started) migration entry: the workload
// owning the migration's instances was torn down mid-flight, so the
// migration can never complete. Closed terminal (Phase=Failed) because
// ledger rows are RECORDS, not work orders — leaving the row Started
// would let the in-flight readers (nextInFlightFromLedger,
// InFlightEntry, HasInFlightMigrationForInstance) resume it as a
// phantom migration on a freshly recreated workload, and keep it
// counted against ValidateCapacity's in-flight cap.
const OutcomeOwnerTornDown = "owner-torn-down"
