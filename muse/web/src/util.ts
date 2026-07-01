// Small shared helpers.

/**
 * Case-insensitive "does any field contain the query?" — the filter behind every
 * panel search box. An empty query matches everything; `undefined` fields are
 * skipped, so callers can spread optional/derived fields freely.
 */
export function matches(query: string, ...fields: (string | undefined)[]): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return fields.some((f) => f != null && f.toLowerCase().includes(q));
}
