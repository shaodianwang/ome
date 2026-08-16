# Contributing to OME

See [CLAUDE.md](CLAUDE.md) for development commands, testing requirements, and
repository conventions.

## Git author email

GitHub recommends using your GitHub-provided `noreply` email address for commit
authorship instead of exposing your actual email address. Before committing,
configure an address ending in `@users.noreply.github.com`:

```sh
github_email="$(gh api user --jq '"\(.id)+\(.login)@users.noreply.github.com"')"
git config --global user.email "$github_email"
```

Install the repository hooks with `pre-commit install`. The local hook checks
the configured author email, and CI checks the author email recorded in each
new commit.
