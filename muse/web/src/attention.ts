// Click-through routing for triage findings, shared by every panel that
// renders the attention feed.

import type { AttentionItem } from "./api";
import { isvcSelected } from "./resource";
import type { Selected } from "./resource";

export interface AttentionHandlers {
  openDeployment: (sel: Selected) => void;
  openModel: (sel: Selected) => void;
  openCapacityCluster: (cluster: string) => void;
}

/** Route a finding to the page that diagnoses it: cluster findings to Capacity,
 *  model findings to the model detail (kind-aware), everything else to the
 *  deployment. */
export function openAttentionItem(item: AttentionItem, handlers: AttentionHandlers): void {
  if (item.category === "cluster") {
    handlers.openCapacityCluster(item.cluster);
  } else if (item.category === "model") {
    handlers.openModel({
      cluster: item.cluster,
      kind: item.kind === "ClusterBaseModel" ? "ClusterBaseModel" : "BaseModel",
      name: item.name,
      namespace: item.namespace,
    });
  } else {
    handlers.openDeployment(isvcSelected(item.cluster, item.name, item.namespace));
  }
}
