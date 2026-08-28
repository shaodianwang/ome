// A generic sortable, filterable, paginated resource table. The Deployments and
// Models pages are thin configurations of this, so they look and behave
// identically — only their columns, tabs, and row-click differ.
import { useEffect, useState } from "react";
import type { ReactNode } from "react";

import { ClusterFilter, EmptyRow, Pager, SearchInput } from "./components";
import { Search } from "./icons";
import type { Tone } from "./types";
import { emptyText, matches, num } from "./util";
import { Dot } from "./viz";

export interface Column<T> {
  key: string;
  label: string;
  /** Right-align the header and cell. */
  right?: boolean;
  /** `className` for the `<td>` (width/typography). */
  cellClass?: string;
  render: (row: T) => ReactNode;
  /** Sort comparator value; omit to make the column non-sortable. */
  sort?: (row: T) => string | number;
}

export interface Tab<T> {
  key: string;
  label: string;
  /** Rows this tab includes (the "all" tab returns `true`). Also drives the count. */
  match: (row: T) => boolean;
  /** Message when this tab has no rows (and no active search). */
  empty?: string;
}

export function ResourceTable<T>({
  title,
  subtitle,
  actions,
  rows,
  status,
  noun,
  searchPlaceholder,
  searchFields,
  clusterOf,
  rowKey,
  rowTone,
  onRowClick,
  columns,
  tabs,
  filter,
  setFilter,
  cluster: clusterProp,
  setCluster: setClusterProp,
  query: queryProp,
  setQuery: setQueryProp,
}: {
  title: string;
  subtitle: string;
  actions?: ReactNode;
  rows: T[];
  status: { loading: boolean; error: string | null };
  noun: string;
  searchPlaceholder: string;
  searchFields: (row: T) => (string | undefined)[];
  clusterOf: (row: T) => string;
  rowKey: (row: T) => string;
  rowTone: (row: T) => Tone;
  onRowClick: (row: T) => void;
  columns: Column<T>[];
  tabs: Tab<T>[];
  filter: string;
  setFilter: (f: string) => void;
  /** Optional controlled cluster filter + search — URL-backed on Deployments, so
   *  they survive navigating into a detail and back. Absent → internal state. */
  cluster?: string;
  setCluster?: (c: string) => void;
  query?: string;
  setQuery?: (q: string) => void;
}) {
  const [qState, setQState] = useState("");
  const [clusterState, setClusterState] = useState("");
  const q = queryProp ?? qState;
  const setQ = setQueryProp ?? setQState;
  const cluster = clusterProp ?? clusterState;
  const setCluster = setClusterProp ?? setClusterState;
  const [sortKey, setSortKey] = useState(columns.find((c) => c.sort)?.key ?? "");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(25);

  // Back to the first page whenever the result set changes.
  useEffect(() => {
    setPage(0);
  }, [q, cluster, filter, pageSize]);

  const clusters = [...new Set(rows.map(clusterOf))].sort();
  const scoped = rows.filter((r) => cluster === "" || clusterOf(r) === cluster);
  const activeTab = tabs.find((t) => t.key === filter) ?? tabs[0];
  const filtered = scoped.filter(
    (r) => (activeTab ? activeTab.match(r) : true) && matches(q, ...searchFields(r)),
  );

  const sortCol = columns.find((c) => c.key === sortKey && c.sort);
  const dir = sortDir === "asc" ? 1 : -1;
  const sorted = sortCol
    ? [...filtered].sort((a, b) => {
        const av = sortCol.sort!(a);
        const bv = sortCol.sort!(b);
        return av < bv ? -dir : av > bv ? dir : 0;
      })
    : filtered;

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const clamped = Math.min(page, totalPages - 1);
  const startIdx = clamped * pageSize;
  const pageRows = sorted.slice(startIdx, startIdx + pageSize);

  const toggleSort = (k: string) => {
    if (k === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(k);
      setSortDir("asc");
    }
  };

  return (
    <section className="panel active">
      <div className="page-head">
        <div>
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
        {actions ? <div className="actions">{actions}</div> : null}
      </div>

      <div className="filterbar">
        <div className="seg" role="group" aria-label={`${title} status filter`}>
          {tabs.map((t) => (
            <button
              key={t.key}
              className={filter === t.key ? "active" : undefined}
              aria-pressed={filter === t.key}
              onClick={() => setFilter(t.key)}
            >
              {t.label} <span className="seg-count">{num(scoped.filter(t.match).length)}</span>
            </button>
          ))}
        </div>
        <ClusterFilter clusters={clusters} value={cluster} onChange={setCluster} />
        <SearchInput
          ariaLabel={`Search ${noun}`}
          placeholder={searchPlaceholder}
          value={q}
          onChange={setQ}
          icon={<Search aria-hidden />}
        />
      </div>

      <div className="card">
        <div className="tbl-wrap">
          <table className="tbl">
            <thead>
              <tr>
                <th className="col-dot"></th>
                {columns.map((c) =>
                  c.sort ? (
                    <th
                      key={c.key}
                      className={`sortable${c.right ? " right" : ""}`}
                      aria-sort={
                        sortKey === c.key
                          ? sortDir === "asc"
                            ? "ascending"
                            : "descending"
                          : undefined
                      }
                    >
                      <button type="button" onClick={() => toggleSort(c.key)}>
                        {/* On right-aligned columns the arrow goes before the label so
                            the label stays flush right, aligning with the numeric cell. */}
                        {c.right ? (
                          <>
                            <span className="sort-arrow" aria-hidden="true">
                              {sortKey === c.key ? (sortDir === "asc" ? "▲" : "▼") : ""}
                            </span>
                            {c.label}
                          </>
                        ) : (
                          <>
                            {c.label}
                            <span className="sort-arrow" aria-hidden="true">
                              {sortKey === c.key ? (sortDir === "asc" ? "▲" : "▼") : ""}
                            </span>
                          </>
                        )}
                      </button>
                    </th>
                  ) : (
                    <th key={c.key} className={c.right ? "right" : undefined}>
                      {c.label}
                    </th>
                  ),
                )}
              </tr>
            </thead>
            <tbody>
              {pageRows.map((r) => (
                <tr key={rowKey(r)} className="rowlink" onClick={() => onRowClick(r)}>
                  <td>
                    <Dot tone={rowTone(r)} />
                  </td>
                  {columns.map((c, index) => (
                    <td key={c.key} className={c.cellClass}>
                      {index === 0 ? (
                        <button
                          type="button"
                          className="row-open"
                          onClick={(event) => {
                            event.stopPropagation();
                            onRowClick(r);
                          }}
                        >
                          {c.render(r)}
                        </button>
                      ) : (
                        c.render(r)
                      )}
                    </td>
                  ))}
                </tr>
              ))}
              {sorted.length === 0 ? (
                <EmptyRow colSpan={columns.length + 1}>
                  {q.trim() && scoped.length
                    ? `No ${noun} match "${q}"`
                    : (activeTab?.empty ?? emptyText(status, `Loading ${noun}…`, `No ${noun}`))}
                </EmptyRow>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <Pager
        total={sorted.length}
        page={clamped}
        totalPages={totalPages}
        pageSize={pageSize}
        start={startIdx}
        noun={noun}
        onPage={setPage}
        onPageSize={setPageSize}
      />
    </section>
  );
}
