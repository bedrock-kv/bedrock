---
name: hunt-dead-code
description: Find and remove transitively dead code - modules reachable only from other dead modules and their own tests. Use when hunting unused code, cleaning up abandoned subsystems, or auditing what the codebase no longer needs.
---

# Hunt Dead Code

Dead means **unreachable from the declared roots**, where tests are not roots. A
module reachable only from other dead modules and from its own tests is dead, no
matter how many incoming edges it has.

That transitivity is the point. Counting incoming references cannot find a
cluster of modules that reference each other but that nothing outside reaches --
every member has callers, so every member looks alive. `ChunkWriter` is the easy
version of this (a leaf, zero incoming edges); an abandoned three-module
subsystem with its own test suite is the version that hides.

The governing rule: **static analysis proposes, the compiler and test suite
dispose.** Never delete on the analyzer's say-so. It cannot resolve dynamic
dispatch, and this codebase is full of it.

## 1. Run the analyzer

```bash
elixir .claude/skills/hunt-dead-code/scripts/dead_code.exs
```

`--json` for machine-readable output, `--root DIR` to analyze elsewhere.

It reports five things, in descending order of confidence:

| Section | Confidence | Action |
|---|---|---|
| Dead clusters | High -- verify then delete | Work one cluster at a time |
| Tests to delete outright | High | Delete with their cluster |
| Tests needing surgical edits | High | Remove only the dead `describe` blocks |
| Unused public API | Advisory | Human judgement, never auto-delete |
| Unreferenced / test-only functions | Advisory | Read each one |

## 2. Triage before touching anything

For each cluster, rule out every false-positive class in
[reference.md](reference.md) before proceeding. The short version:

- Is it referenced from inside a `quote` block anywhere? (analyzer handles this,
  but confirm for anything surprising)
- Is it a behaviour impl selected by config or by an atom at runtime?
- Is it named as a bare atom, in a config file, or in a `priv/` schema?
- Is it attached as a telemetry handler?
- Is it a supervision-tree child built from capabilities?
- Does `git log --follow` show it arriving recently as scaffolding for
  in-flight work?

```bash
rg 'ModuleName|:module_name' --type elixir --type markdown
git log --oneline --follow -- path/to/module.ex
```

A cluster that survives triage is a **candidate**, not a corpse.

## 3. Ticket and branch

Every code change gets a ticket (see CLAUDE.md).

```bash
bw create "Remove dead <cluster name>" -p P3
git checkout -b <ticket-id>-remove-<slug>
```

One cluster per ticket. Clusters are independent; batching them makes a red gate
ambiguous about which removal caused it.

## 4. Delete the whole unit

A cluster's members only make sense together -- delete them together, along with:

- every test file listed under **Tests to delete outright**
- the dead `describe` / `test` blocks in files under **Tests needing surgical
  edits** (leave the rest of the file intact)
- any now-unused `alias` lines left behind in surviving files
- entries in `mix.exs` `docs/0` (`extras`, `skip_code_autolink_to`) that named
  the deleted modules
- guide prose in `guides/` that documents the removed behaviour

```bash
git rm lib/path/to/module.ex test/path/to/module_test.exs
```

## 5. Run the verification gate

This is what actually proves deadness. Run all of it.

```bash
mix compile --force --warnings-as-errors
mix quality          # format --check-formatted, credo --strict, dialyzer
mix test
```

Then the distributed durability suite, which exercises the runtime wiring that
static analysis and unit tests both miss -- the recovery phases, the supervision
tree, the control plane. This is where a wrongly-removed dynamically-dispatched
module surfaces.

```bash
# once per machine
MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest

BEDROCK_INCLUDE_DISTRIBUTED=1 mix test --include distributed \
  test/bedrock/distributed/minio_durability_test.exs
```

Skipping the distributed suite for control-plane, recovery, or supervision-tree
clusters makes the gate meaningless for exactly the code most likely to be
dynamically wired.

## 6. Land it, or record why it lives

**Green** -- the removal is proven. Commit, close the ticket, sync.

**Red** -- the module was alive and the analyzer could not see how. Do not patch
the analyzer for the special case. Revert the deletion and add the entry point to
`roots.exs` **with the reason it is reachable**:

```elixir
roots: [
  {"lib/bedrock/some/module.ex", "Attached as a telemetry handler by Foo.start/0"}
]
```

This is the ratchet, and it is the most valuable thing the skill produces. Each
false positive becomes a permanent, justified root: the noise floor drops for
good, and `roots.exs` accumulates into documentation of the dynamic-wiring
surface -- knowledge this codebase records nowhere else. An analyzer tweak would
have bought a quieter report and taught no one anything.

## Handling the advisory sections

**Unused public API.** Bedrock is a published package, so a public module can
never be *proven* dead from inside it -- a downstream user may call it. The
analyzer flags public modules with no internal caller and stops there. Decide per
module whether it is intended API or drift. Removing one is a breaking change:
it needs its own ticket and a CHANGELOG entry.

**Unreferenced public functions** (name appears in no lib or test file) is the
stronger tier. **Test-only functions** (name appears only in tests) are dead by
this skill's definition, but that is also the exact shape of a helper kept
deliberately for testing -- read each one before removing it.

Both tiers are name-based and arity-insensitive. They miss nothing that is
called, but they do flag things reached by dynamic dispatch, protocols, and
macro-generated calls. Treat them as a reading list, not a work queue.

## Files

- `scripts/dead_code.exs` -- the analyzer
- `roots.exs` -- declared roots; the ratchet, grown over time
- `reference.md` -- false-positive taxonomy and why `mix xref` is not enough

> `.claude/` is gitignored in this repo, so `roots.exs` is local-only. The
> justifications accumulated there are real project knowledge and will not
> survive a fresh clone. If that becomes a problem, move the manifest somewhere
> tracked and point the analyzer's `manifest_path` at it.
