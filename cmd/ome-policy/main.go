// ome-policy is the CI/preview companion for AutoscalerPolicy objects. It
// links the same render package as the controller and the admission webhook,
// so a policy that validates and renders here behaves identically in the
// cluster — the point of the tool is that CI preview and runtime cannot
// disagree.
//
// Subcommands:
//
//	check  --policies <path>... --isvcs <path>... [--provider name=addr]... [--require-ref <path>...]
//	       Validates every policy, renders every (policy, referencing
//	       component) pair, and fails on any issue. --require-ref
//	       additionally forbids inline KEDA blocks on components in the
//	       given manifests unless the ISVC carries the exemption
//	       annotation ome.io/inline-autoscaler-exempt.
//	render --policy <file> --isvc <file> --component <engine|decoder|router> [--provider name=addr]...
//	       Prints the rendered ComponentAutoscaler as YAML plus both digests.
//	digest --policy <file>
//	       Prints the portable digest (compare against the stored object's
//	       status.portableDigest to verify distribution).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/autoscalerpolicy/render"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
)

// inlineExemptAnnotation marks an ISVC whose inline KEDA blocks are an
// approved exception under a --require-ref ratchet. CI-only contract; the
// controller never reads it.
const inlineExemptAnnotation = "ome.io/inline-autoscaler-exempt"

const placeholderProviderAddress = "http://provider.placeholder.invalid"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "check":
		err = runCheck(os.Args[2:])
	case "render":
		err = runRender(os.Args[2:])
	case "digest":
		err = runDigest(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ome-policy <check|render|digest> [flags]")
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func runCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	var policyPaths, isvcPaths, providerFlags, requireRefPaths multiFlag
	fs.Var(&policyPaths, "policies", "policy manifest file or directory (repeatable)")
	fs.Var(&isvcPaths, "isvcs", "InferenceService manifest file or directory (repeatable)")
	fs.Var(&providerFlags, "provider", "name=serverAddress binding (repeatable); unreferenced names are ignored")
	fs.Var(&requireRefPaths, "require-ref", "manifest file or directory in which inline KEDA blocks are forbidden without the exemption annotation (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	policies, err := loadPolicies(policyPaths)
	if err != nil {
		return err
	}
	isvcs, err := loadISVCs(isvcPaths)
	if err != nil {
		return err
	}

	failures := 0
	fail := func(format string, a ...any) {
		failures++
		fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", a...)
	}

	byName := map[string]*v1beta1.AutoscalerPolicy{}
	for _, policy := range policies {
		if issues := render.ValidateSpec(&policy.Spec); len(issues) > 0 {
			for _, issue := range issues {
				fail("policy %s: %s", policy.Name, issue)
			}
			continue
		}
		digest, err := render.PortableDigest(&policy.Spec)
		if err != nil {
			fail("policy %s: %v", policy.Name, err)
			continue
		}
		fmt.Printf("policy %s/%s valid portableDigest=%s\n", policy.Namespace, policy.Name, digest)
		byName[policy.Namespace+"/"+policy.Name] = policy
	}

	providers := providersFromFlags(providerFlags)
	for _, isvc := range isvcs {
		for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
			ref := autoscaler.ComponentPolicyRef(isvc, component)
			if ref == nil {
				continue
			}
			policy, ok := byName[isvc.Namespace+"/"+ref.Name]
			if !ok {
				fail("%s/%s %s: ref %q does not match any provided (valid) policy", isvc.Namespace, isvc.Name, component, ref.Name)
				continue
			}
			result, err := renderFor(policy, isvc, component, providers)
			if err != nil {
				fail("%s/%s %s: %v", isvc.Namespace, isvc.Name, component, err)
				continue
			}
			fmt.Printf("%s/%s %s -> policy %s resolvedDigest=%s\n", isvc.Namespace, isvc.Name, component, ref.Name, result.ResolvedDigest)
		}
	}

	if len(requireRefPaths) > 0 {
		ratchet, err := loadISVCs(requireRefPaths)
		if err != nil {
			return err
		}
		for _, isvc := range ratchet {
			if isvc.Annotations[inlineExemptAnnotation] == "true" {
				continue
			}
			for component, ext := range componentExts(isvc) {
				if ext.Autoscaler != nil && ext.Autoscaler.Class == v1beta1.AutoscalerKEDA {
					fail("%s/%s %s: inline KEDA block without a policy ref (annotate %s=true for an approved exception)",
						isvc.Namespace, isvc.Name, component, inlineExemptAnnotation)
				}
			}
		}
	}

	if failures > 0 {
		return fmt.Errorf("%d check failure(s)", failures)
	}
	fmt.Println("all checks passed")
	return nil
}

func runRender(args []string) error {
	fs := flag.NewFlagSet("render", flag.ExitOnError)
	policyPath := fs.String("policy", "", "policy manifest file")
	isvcPath := fs.String("isvc", "", "InferenceService manifest file")
	component := fs.String("component", "engine", "component to render for")
	var providerFlags multiFlag
	fs.Var(&providerFlags, "provider", "name=serverAddress binding (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	policies, err := loadPolicies(multiFlag{*policyPath})
	if err != nil {
		return err
	}
	isvcs, err := loadISVCs(multiFlag{*isvcPath})
	if err != nil {
		return err
	}
	if len(policies) != 1 || len(isvcs) != 1 {
		return fmt.Errorf("render expects exactly one policy and one InferenceService")
	}
	result, err := renderFor(policies[0], isvcs[0], v1beta1.ComponentType(*component), providersFromFlags(providerFlags))
	if err != nil {
		return err
	}
	encoded, err := yaml.Marshal(result.Autoscaler)
	if err != nil {
		return err
	}
	fmt.Printf("# portableDigest: %s\n# resolvedDigest: %s\n%s", result.PortableDigest, result.ResolvedDigest, encoded)
	return nil
}

func runDigest(args []string) error {
	fs := flag.NewFlagSet("digest", flag.ExitOnError)
	policyPath := fs.String("policy", "", "policy manifest file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	policies, err := loadPolicies(multiFlag{*policyPath})
	if err != nil {
		return err
	}
	for _, policy := range policies {
		digest, err := render.PortableDigest(&policy.Spec)
		if err != nil {
			return err
		}
		fmt.Printf("%s/%s %s\n", policy.Namespace, policy.Name, digest)
	}
	return nil
}

// renderFor renders one (policy, component) pair against the ISVC's authored
// bounds. Providers referenced by the policy but not supplied on the command
// line bind to a placeholder address: CI validates templates and typed
// derivation; the real endpoint is cluster-local configuration by design.
func renderFor(policy *v1beta1.AutoscalerPolicy, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, providers render.Providers) (*render.Result, error) {
	ext, ok := componentExts(isvc)[component]
	if !ok {
		return nil, fmt.Errorf("component %s is not declared on %s/%s", component, isvc.Namespace, isvc.Name)
	}
	bounds := autoscaler.EffectiveComponentBounds(ext)
	bound := render.Providers{}
	if policy.Spec.Keda != nil {
		for _, trigger := range policy.Spec.Keda.Triggers {
			if trigger.ProviderRef == nil || trigger.ProviderRef.Name == "" {
				continue
			}
			if binding, ok := providers[trigger.ProviderRef.Name]; ok {
				bound[trigger.ProviderRef.Name] = binding
			} else {
				bound[trigger.ProviderRef.Name] = render.ProviderBinding{ServerAddress: placeholderProviderAddress}
			}
		}
	}
	return render.RenderWithCache(render.NewCache(), policy, bound, render.Context{
		Namespace:   isvc.Namespace,
		ISVCName:    isvc.Name,
		Component:   string(component),
		MinReplicas: bounds.Min,
		MaxReplicas: bounds.Max,
		TargetName:  isvc.Name + "-" + string(component),
	})
}

func componentExts(isvc *v1beta1.InferenceService) map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec {
	out := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{}
	if isvc.Spec.Engine != nil {
		out[v1beta1.EngineComponent] = &isvc.Spec.Engine.ComponentExtensionSpec
	}
	if isvc.Spec.Decoder != nil {
		out[v1beta1.DecoderComponent] = &isvc.Spec.Decoder.ComponentExtensionSpec
	}
	if isvc.Spec.Router != nil {
		out[v1beta1.RouterComponent] = &isvc.Spec.Router.ComponentExtensionSpec
	}
	return out
}

func providersFromFlags(flags multiFlag) render.Providers {
	providers := render.Providers{}
	for _, entry := range flags {
		name, address, ok := strings.Cut(entry, "=")
		if ok && name != "" && address != "" {
			providers[name] = render.ProviderBinding{ServerAddress: address}
		}
	}
	return providers
}

func loadPolicies(paths multiFlag) ([]*v1beta1.AutoscalerPolicy, error) {
	var out []*v1beta1.AutoscalerPolicy
	err := eachDocument(paths, func(doc []byte) error {
		if kindOf(doc) != "AutoscalerPolicy" {
			return nil
		}
		policy := &v1beta1.AutoscalerPolicy{}
		if err := yaml.UnmarshalStrict(doc, policy); err != nil {
			return err
		}
		out = append(out, policy)
		return nil
	})
	return out, err
}

func loadISVCs(paths multiFlag) ([]*v1beta1.InferenceService, error) {
	var out []*v1beta1.InferenceService
	err := eachDocument(paths, func(doc []byte) error {
		if kindOf(doc) != "InferenceService" {
			return nil
		}
		isvc := &v1beta1.InferenceService{}
		if err := yaml.Unmarshal(doc, isvc); err != nil {
			return err
		}
		out = append(out, isvc)
		return nil
	})
	return out, err
}

func kindOf(doc []byte) string {
	var meta struct {
		Kind string `json:"kind"`
	}
	_ = yaml.Unmarshal(doc, &meta)
	return meta.Kind
}

func eachDocument(paths multiFlag, visit func([]byte) error) error {
	for _, root := range paths {
		if root == "" {
			continue
		}
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, doc := range strings.Split(string(content), "\n---") {
				if strings.TrimSpace(doc) == "" {
					continue
				}
				if err := visit([]byte(doc)); err != nil {
					return fmt.Errorf("%s: %w", path, err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
