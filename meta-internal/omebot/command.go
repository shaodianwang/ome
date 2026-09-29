//go:build meta_internal

package omebot

import (
	"sort"
	"strings"
)

// Kind classifies a parsed command.
type Kind string

const (
	KindHelp     Kind = "help"
	KindClusters Kind = "clusters"
	KindCLI      Kind = "cli"
)

// Parsed is a validated command ready to run.
type Parsed struct {
	Kind Kind
	// Verb is the kubectl-ome verb, one or two words, for KindCLI.
	Verb []string
	// Args are the remaining CLI arguments with --cluster removed.
	Args []string
	// Cluster is the registry name the command targets; empty means every
	// cluster when the verb allows fan-out.
	Cluster string
}

// readVerbs is the closed set of kubectl-ome verbs the bot will run. Every
// entry is read-only; guarded verbs never appear here, so no prompt or
// caller can reach them through this service.
var readVerbs = map[string]bool{
	"get":                   true,
	"status":                true,
	"logs":                  true,
	"version":               true,
	"rollout status":        true,
	"rollout explain":       true,
	"rollout history":       true,
	"rollout validate":      true,
	"traffic status":        true,
	"traffic explain":       true,
	"placement status":      true,
	"placement explain":     true,
	"placement endpoint":    true,
	"quota status":          true,
	"quota tree":            true,
	"quota validate":        true,
	"instance list":         true,
	"instance status":       true,
	"instance retry-blocks": true,
	"migration status":      true,
	"migration history":     true,
	"runtime explain":       true,
	"runtime effective":     true,
	"runtime tree":          true,
	"runtime history":       true,
	"autoscale status":      true,
	"autoscale explain":     true,
	"accelerator explain":   true,
	"admin doctor":          true,
	"admin recommendations": true,
	"cluster status":        true,
}

// fanoutVerbs may run on every registry cluster when --cluster is absent;
// they are cheap lookups whose union is the useful answer.
var fanoutVerbs = map[string]bool{
	"get":    true,
	"status": true,
}

// forbiddenFlags would let a caller change which identity or endpoint the
// CLI talks to, or bypass a guard. They are rejected regardless of position.
var forbiddenFlags = []string{
	"--kubeconfig", "--context", "--cluster-name", "--user", "--as", "--as-group", "--as-uid",
	"--token", "--server", "--tls-server-name", "--client-certificate", "--client-key",
	"--certificate-authority", "--insecure-skip-tls-verify", "--username", "--password",
	"--cache-dir", "--yes", "--exec",
}

// ReadVerbs lists the allowed verbs for help output.
func ReadVerbs() []string {
	out := make([]string, 0, len(readVerbs))
	for v := range readVerbs {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// ParseCommand turns the text after "/ome" into a Parsed command.
func ParseCommand(text string) (Parsed, error) {
	tokens, err := tokenize(text)
	if err != nil {
		return Parsed{}, err
	}
	if len(tokens) == 0 {
		return Parsed{Kind: KindHelp}, nil
	}
	switch strings.ToLower(tokens[0]) {
	case "help", "-h", "--help":
		return Parsed{Kind: KindHelp}, nil
	case "clusters":
		return Parsed{Kind: KindClusters}, nil
	case "installed":
		if len(tokens) == 2 && strings.EqualFold(tokens[1], "clusters") {
			return Parsed{Kind: KindClusters}, nil
		}
		return Parsed{}, refuse(ErrNotAVerb, "unknown command %q; try `help`", strings.Join(tokens, " "))
	}

	verbLen := 0
	if len(tokens) >= 2 && readVerbs[tokens[0]+" "+tokens[1]] {
		verbLen = 2
	} else if readVerbs[tokens[0]] {
		verbLen = 1
	}
	if verbLen == 0 {
		return Parsed{}, refuse(ErrNotAVerb, "%q is not a read-only kubectl-ome verb this bot runs; try `help`", strings.Join(tokens[:min(2, len(tokens))], " "))
	}
	p := Parsed{Kind: KindCLI, Verb: tokens[:verbLen]}
	rest := tokens[verbLen:]
	for i := 0; i < len(rest); i++ {
		tok := rest[i]
		switch {
		case tok == "--cluster":
			if i+1 >= len(rest) {
				return Parsed{}, refuse(ErrMalformed, "--cluster needs a value")
			}
			p.Cluster = rest[i+1]
			i++
			continue
		case strings.HasPrefix(tok, "--cluster="):
			p.Cluster = strings.TrimPrefix(tok, "--cluster=")
			continue
		}
		for _, f := range forbiddenFlags {
			if tok == f || strings.HasPrefix(tok, f+"=") {
				return Parsed{}, refuse(ErrForbiddenFlag, "flag %s is not allowed through this bot", f)
			}
		}
		p.Args = append(p.Args, tok)
	}
	if p.Cluster == "" && !fanoutVerbs[strings.Join(p.Verb, " ")] {
		return Parsed{}, refuse(ErrNeedsCluster, "`%s` needs `--cluster <name>`; `clusters` lists them", strings.Join(p.Verb, " "))
	}
	return p, nil
}

// tokenize splits like a POSIX shell for quotes and whitespace, without any
// expansion, and refuses control characters.
func tokenize(text string) ([]string, error) {
	var (
		tokens []string
		cur    strings.Builder
		inTok  bool
		quote  rune
	)
	flush := func() {
		if inTok {
			tokens = append(tokens, cur.String())
			cur.Reset()
			inTok = false
		}
	}
	for _, r := range text {
		if r < 0x20 && r != '\t' && r != '\n' || r == 0x7f {
			return nil, refuse(ErrMalformed, "control character in command")
		}
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			inTok = true
		case r == ' ' || r == '\t' || r == '\n':
			flush()
		default:
			cur.WriteRune(r)
			inTok = true
		}
	}
	if quote != 0 {
		return nil, refuse(ErrMalformed, "unbalanced quote in command")
	}
	flush()
	return tokens, nil
}

// HelpText is the reply to `help`.
func HelpText(clusters []string) string {
	var b strings.Builder
	b.WriteString("*OME bot* runs read-only kubectl-ome commands against the fleet.\n")
	b.WriteString("• `installed clusters` — every cluster in the registry and whether OME answers there\n")
	b.WriteString("• `<verb> [args] --cluster <name> [-n <ns>]` — verbs: " + strings.Join(ReadVerbs(), ", ") + "\n")
	b.WriteString("• `get`/`status` without `--cluster` run on every cluster\n")
	if len(clusters) > 0 {
		b.WriteString("Clusters: " + strings.Join(clusters, ", ") + "\n")
	}
	b.WriteString("Nothing here changes a cluster; guarded verbs are refused.")
	return b.String()
}
