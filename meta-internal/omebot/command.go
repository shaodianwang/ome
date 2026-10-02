//go:build meta_internal

package omebot

import (
	"regexp"
	"sort"
	"strings"
)

// Kind classifies a parsed command.
type Kind string

const (
	KindHelp     Kind = "help"
	KindClusters Kind = "clusters"
	KindCLI      Kind = "cli"
	// KindQuestion is free text for the answer worker rather than a verb.
	KindQuestion Kind = "question"
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
	// Question is the text for the answer worker, without its ask or recipe
	// prefix, for KindQuestion.
	Question string
	// QuestionMode is "ask", "recipe", or "" when a question mark alone
	// marked the text as a question.
	QuestionMode string
	// Guarded marks one of kubectl-ome's eleven actions that change a
	// cluster; Confirmed records the caller's --yes.
	Guarded   bool
	Confirmed bool
}

var (
	// helpRE accepts the bare word and the variants people type in chat:
	// "help me", "help?", "?help".
	helpRE = regexp.MustCompile(`(?i)^\??help(?:\s|\?|$)`)
	// questionPrefixRE marks text for the answer worker by its first word.
	questionPrefixRE = regexp.MustCompile(`(?is)^(ask|recipe)\s+(.+)$`)
)

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

// guardedVerbs are kubectl-ome's eleven actions that change a cluster. They
// parse like read verbs and run only under the conditions the server
// enforces: mutations on, a writable cluster, a cleared actor, and --yes.
var guardedVerbs = map[string]bool{
	"rollout pause":         true,
	"rollout resume":        true,
	"rollout promote":       true,
	"rollout rollback":      true,
	"rollout repin":         true,
	"traffic drain":         true,
	"traffic undrain":       true,
	"migration start":       true,
	"instance release-held": true,
	"runtime sync":          true,
	"scale":                 true,
}

// fanoutVerbs may run on every registry cluster when --cluster is absent;
// they are cheap lookups whose union is the useful answer.
var fanoutVerbs = map[string]bool{
	"get":    true,
	"status": true,
}

// forbiddenFlags would let a caller change which identity or endpoint the
// CLI talks to, or bypass a guard. They are rejected regardless of position.
// --yes is handled apart: it is the confirmation a guarded verb needs and
// meaningless, so refused, on a read verb.
var forbiddenFlags = []string{
	"--kubeconfig", "--context", "--cluster-name", "--user", "--as", "--as-group", "--as-uid",
	"--token", "--server", "--tls-server-name", "--client-certificate", "--client-key",
	"--certificate-authority", "--insecure-skip-tls-verify", "--username", "--password",
	"--cache-dir", "--exec",
}

// ReadVerbs lists the allowed verbs for help output.
func ReadVerbs() []string {
	return sortedVerbs(readVerbs)
}

// GuardedVerbs lists the guarded verbs for help output.
func GuardedVerbs() []string {
	return sortedVerbs(guardedVerbs)
}

func sortedVerbs(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// ParseCommand turns the text after the command or mention into a Parsed
// command. Help and questions are recognised on the raw text first, so a
// question keeps its apostrophes and quotes; only a command is tokenized.
func ParseCommand(text string) (Parsed, error) {
	if strings.ContainsFunc(text, isControl) {
		return Parsed{}, refuse(ErrMalformed, "control character in command")
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || helpRE.MatchString(trimmed) {
		return Parsed{Kind: KindHelp}, nil
	}
	if q, ok := parseQuestion(trimmed); ok {
		return q, nil
	}
	tokens, err := tokenize(text)
	if err != nil {
		return Parsed{}, err
	}
	if len(tokens) == 0 {
		return Parsed{Kind: KindHelp}, nil
	}
	switch strings.ToLower(tokens[0]) {
	case "-h", "--help":
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
	if two := strings.Join(tokens[:min(2, len(tokens))], " "); len(tokens) >= 2 && (readVerbs[two] || guardedVerbs[two]) {
		verbLen = 2
	} else if readVerbs[tokens[0]] || guardedVerbs[tokens[0]] {
		verbLen = 1
	}
	if verbLen == 0 {
		return Parsed{}, refuse(ErrNotAVerb, "%q is not a kubectl-ome verb this bot runs; try `help`", strings.Join(tokens[:min(2, len(tokens))], " "))
	}
	verb := strings.Join(tokens[:verbLen], " ")
	p := Parsed{Kind: KindCLI, Verb: tokens[:verbLen], Guarded: guardedVerbs[verb]}
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
		case tok == "--yes":
			if !p.Guarded {
				return Parsed{}, refuse(ErrForbiddenFlag, "flag --yes is not allowed with a read verb")
			}
			p.Confirmed = true
		}
		for _, f := range forbiddenFlags {
			if tok == f || strings.HasPrefix(tok, f+"=") {
				return Parsed{}, refuse(ErrForbiddenFlag, "flag %s is not allowed through this bot", f)
			}
		}
		p.Args = append(p.Args, tok)
	}
	if p.Cluster == "" && p.Guarded {
		return Parsed{}, refuse(ErrNeedsCluster, "`%s` changes a cluster and needs `--cluster <name>`; `clusters` lists them", verb)
	}
	if p.Cluster == "" && !fanoutVerbs[verb] {
		return Parsed{}, refuse(ErrNeedsCluster, "`%s` needs `--cluster <name>`; `clusters` lists them", verb)
	}
	return p, nil
}

// parseQuestion recognises text for the answer worker: an ask or recipe
// prefix, or a question mark anywhere in the text.
func parseQuestion(text string) (Parsed, bool) {
	if m := questionPrefixRE.FindStringSubmatch(text); m != nil {
		return Parsed{Kind: KindQuestion, Question: strings.TrimSpace(m[2]), QuestionMode: strings.ToLower(m[1])}, true
	}
	if strings.Contains(text, "?") {
		return Parsed{Kind: KindQuestion, Question: text}, true
	}
	return Parsed{}, false
}

// isControl reports a character no command or question may carry; tab and
// newline are ordinary whitespace.
func isControl(r rune) bool {
	return r < 0x20 && r != '\t' && r != '\n' || r == 0x7f
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
		if isControl(r) {
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

// HelpText is the reply to `help`. writable names the clusters that admit
// guarded verbs; empty means none does.
func HelpText(clusters, writable []string) string {
	var b strings.Builder
	b.WriteString("*OME bot* runs read-only kubectl-ome commands against the fleet.\n")
	b.WriteString("• `installed clusters` — every cluster in the registry and whether OME answers there\n")
	b.WriteString("• `<verb> [args] --cluster <name> [-n <ns>]` — verbs: " + strings.Join(ReadVerbs(), ", ") + "\n")
	b.WriteString("• `get`/`status` without `--cluster` run on every cluster\n")
	if len(clusters) > 0 {
		b.WriteString("Clusters: " + strings.Join(clusters, ", ") + "\n")
	}
	if len(writable) == 0 {
		b.WriteString("Read verbs never change a cluster; no cluster here admits guarded verbs.")
		return b.String()
	}
	b.WriteString("• guarded verbs (" + strings.Join(GuardedVerbs(), ", ") + ") run only on " +
		strings.Join(writable, ", ") + ", only for the oncall, only with `--yes`; add `--dry-run=server` to preview\n")
	b.WriteString("Read verbs never change a cluster.")
	return b.String()
}
