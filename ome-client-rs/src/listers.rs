//! Typed listers over the informer cache — the `pkg/client/listers` analog.
//!
//! A `Lister<K>` reads a reflector `Store<K>` (the cache): cluster-wide or
//! per-namespace get/list, plus label-selector filtering (exact equality and
//! full set-based `LabelSelector`). Generic over `K`, so one impl serves every
//! kind — there is no per-type generated lister.
//!
//! Note vs client-go listers: the `Store` is keyed by namespace/name only, so
//! `list_namespaced` / `list_matching` / `list_by_selector` filter the cached
//! set (O(n)) rather than using a secondary index. For the read-plane's sizes
//! that is fine; a real secondary index can be added later if a hot path needs
//! it.

use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Resource;

pub use crate::informers::Store;

/// Read-only typed accessor over a reflector `Store` (the lister).
#[derive(Clone)]
pub struct Lister<K>
where
    K: Resource + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    store: Store<K>,
}

impl<K> Lister<K>
where
    K: Resource + Clone + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    pub fn new(store: Store<K>) -> Self {
        Self { store }
    }

    /// Every cached object (cluster-wide).
    pub fn list(&self) -> Vec<Arc<K>> {
        self.store.state()
    }

    /// Cached objects in a namespace.
    pub fn list_namespaced(&self, ns: &str) -> Vec<Arc<K>> {
        self.store
            .state()
            .into_iter()
            .filter(|o| o.meta().namespace.as_deref() == Some(ns))
            .collect()
    }

    /// Cached objects whose labels match all of `selector` (exact equality).
    pub fn list_matching(&self, selector: &BTreeMap<String, String>) -> Vec<Arc<K>> {
        self.store
            .state()
            .into_iter()
            .filter(|o| {
                let labels = o.meta().labels.as_ref();
                selector
                    .iter()
                    .all(|(k, v)| labels.and_then(|l| l.get(k)).is_some_and(|x| x == v))
            })
            .collect()
    }

    /// Cached objects matching a full `LabelSelector` — `matchLabels` plus
    /// `matchExpressions` (`In`/`NotIn`/`Exists`/`DoesNotExist`), the
    /// `labels.Selector` analog. An empty selector matches everything.
    pub fn list_by_selector(&self, selector: &LabelSelector) -> Vec<Arc<K>> {
        self.store
            .state()
            .into_iter()
            .filter(|o| label_selector_matches(selector, o.meta().labels.as_ref()))
            .collect()
    }

    /// [`list_by_selector`](Self::list_by_selector) scoped to one namespace.
    pub fn list_namespaced_by_selector(&self, ns: &str, selector: &LabelSelector) -> Vec<Arc<K>> {
        self.store
            .state()
            .into_iter()
            .filter(|o| {
                o.meta().namespace.as_deref() == Some(ns)
                    && label_selector_matches(selector, o.meta().labels.as_ref())
            })
            .collect()
    }

    /// First cached object with this name (use for cluster-scoped kinds).
    pub fn get(&self, name: &str) -> Option<Arc<K>> {
        self.store
            .state()
            .into_iter()
            .find(|o| o.meta().name.as_deref() == Some(name))
    }

    /// Cached object by namespace + name (for namespaced kinds).
    pub fn get_namespaced(&self, ns: &str, name: &str) -> Option<Arc<K>> {
        self.store.state().into_iter().find(|o| {
            o.meta().name.as_deref() == Some(name) && o.meta().namespace.as_deref() == Some(ns)
        })
    }

    /// The underlying `Store`, for `ObjectRef`-indexed gets or `wait_until_ready`.
    pub fn store(&self) -> &Store<K> {
        &self.store
    }
}

/// Client-side evaluation of a Kubernetes `LabelSelector`, matching apimachinery
/// semantics: a selector with no `matchLabels`/`matchExpressions` matches every
/// object; otherwise every requirement must hold.
pub fn label_selector_matches(
    selector: &LabelSelector,
    labels: Option<&BTreeMap<String, String>>,
) -> bool {
    let empty = BTreeMap::new();
    let labels = labels.unwrap_or(&empty);
    if let Some(match_labels) = &selector.match_labels {
        if !match_labels.iter().all(|(k, v)| labels.get(k) == Some(v)) {
            return false;
        }
    }
    if let Some(exprs) = &selector.match_expressions {
        for req in exprs {
            let present = labels.get(&req.key);
            let in_values = || {
                req.values
                    .as_ref()
                    .zip(present)
                    .is_some_and(|(vs, x)| vs.contains(x))
            };
            let matched = match req.operator.as_str() {
                "In" => in_values(),
                "NotIn" => !in_values(),
                "Exists" => present.is_some(),
                "DoesNotExist" => present.is_none(),
                _ => false, // unknown operator: conservatively no match
            };
            if !matched {
                return false;
            }
        }
    }
    true
}
