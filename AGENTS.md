# OME contributor guidance

Read and follow [CLAUDE.md](CLAUDE.md) for repository architecture, development
commands, and project-specific constraints.

## Git author email

GitHub recommends using a GitHub-provided `noreply` email address as your Git
author identity instead of exposing your actual email address. Configure Git
with the address shown under your GitHub email settings; supported addresses
end in `@users.noreply.github.com`.

The repository's pre-commit hook and CI validate the author email on new
commits. For example, derive the current account's address with GitHub CLI and
set it globally:

```sh
github_email="$(gh api user --jq '"\(.id)+\(.login)@users.noreply.github.com"')"
git config --global user.email "$github_email"
```
