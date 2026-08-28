/** Kubernetes kinds that the console links to as full resource identities. */
export const RESOURCE_KINDS = [
  "InferenceService",
  "BaseModel",
  "ClusterBaseModel",
  "ServingRuntime",
  "ClusterServingRuntime",
] as const;

export type ResourceKind = (typeof RESOURCE_KINDS)[number];

/** A reference within one workload cluster. Namespace is absent for cluster-scoped kinds. */
export interface ResourceRef {
  kind: ResourceKind;
  name: string;
  namespace?: string;
}

/** The canonical identity used by navigation, API lookups, and cross-resource joins. */
export interface Selected extends ResourceRef {
  cluster: string;
}

/** Old detail URLs did not encode kind/scope. Keep that ambiguity explicit so
 * canonical links never silently become cluster-scoped during parsing. */
export interface LegacySelected {
  cluster: string;
  name: string;
  legacy: "model" | "runtime";
}

export type RouteSelected = Selected | LegacySelected;

export function isResourceKind(value: string | null): value is ResourceKind {
  return value != null && (RESOURCE_KINDS as readonly string[]).includes(value);
}

export function selectResource(cluster: string, ref: ResourceRef): Selected {
  return { cluster, ...ref };
}

/** InferenceService identity from its parts — the console's most common Selected. */
export function isvcSelected(cluster: string, name: string, namespace?: string): Selected {
  return { cluster, kind: "InferenceService", name, namespace };
}

export function isLegacySelected(ref: RouteSelected): ref is LegacySelected {
  return "legacy" in ref;
}

export function selectionKey(ref: RouteSelected): string {
  return isLegacySelected(ref)
    ? [ref.cluster, "legacy", ref.legacy, ref.name].join("\u0000")
    : resourceKey(ref);
}

/** Collision-safe in-memory key; URL encoding is handled separately by the router/client. */
export function resourceKey(ref: Selected): string {
  return [ref.cluster, ref.kind, ref.namespace ?? "", ref.name].join("\u0000");
}

export function sameResource(a: Selected | null | undefined, b: Selected): boolean {
  return a != null && resourceKey(a) === resourceKey(b);
}

export const RESOURCE_PLURALS: Record<ResourceKind, string> = {
  InferenceService: "inferenceservices",
  BaseModel: "basemodels",
  ClusterBaseModel: "clusterbasemodels",
  ServingRuntime: "servingruntimes",
  ClusterServingRuntime: "clusterservingruntimes",
};
