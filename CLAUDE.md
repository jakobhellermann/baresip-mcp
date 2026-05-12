# baresip-mcp

When changing behavior, update the README only for **user-facing** things:
CLI flags, env vars, MCP tool list/semantics, required external setup
(e.g. `~/.baresip/accounts`, `BARESIP_MODPATH`), or the architecture
diagram if the runtime topology changes.

Skip README updates for internal refactors or anything derivable from
`--help`. A small README is a good README.

## Pushing

This repo has no PR workflow — pushing directly to `main` is fine when
the user asks for a push.

If the most recent commit is still fresh (minutes old, not pulled by
anyone else yet) **and** the follow-up is clearly the same change,
squashing into it and force-pushing is allowed — but optional, not
required. For older or unrelated commits, add a new commit instead.
