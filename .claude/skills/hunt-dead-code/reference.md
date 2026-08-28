# Dead-code detection: what defeats it

Reference for the triage step in [SKILL.md](SKILL.md). Every class below was
confirmed in this codebase, not hypothesised.

## Why not `mix xref`

`mix xref graph` is the obvious foundation and it is the wrong one, for a
specific and fatal reason: **it cannot see through `quote`.**

`lib/bedrock/cluster.ex` contains 14 call sites to `Bedrock.Internal.ClusterSupervisor`.
Every one is inside `quote do` in `__using__`, so the reference is injected into
the *downstream user's* module -- code that does not exist in this repository.
The compiler records no edge from `cluster.ex` to `cluster_supervisor.ex`.
`repo.ex` -> `Internal.Repo` is the same story.

Reachability over the xref graph therefore reports the live runtime guts of the
library as dead:

```
ClusterSupervisor, Internal.Repo, all 8 of internal/transaction_builder/,
every tracing.ex  -- 20 files, all alive, all flagged
```

A tool built on xref would confidently tell you to delete the engine.

The analyzer builds its graph from **source AST** instead, walking into `quote`
blocks like any other node. That over-approximates liveness, which is the
correct direction of error: missing some dead code costs a little tidiness,
deleting live code costs a production incident.

Measured on this repo:

| Graph | Reported dead | Of which false positives |
|---|---|---|
| Zero in-degree | 19 | finds only leaves; blind to clusters |
| xref reachability | 28 | 20 |
| AST reachability (quote-aware) | 4 | 0 confirmed so far |

## The false-positive classes

### 1. `quote` / `__using__` injection

Handled by the analyzer. Listed here because it is the largest class and worth
recognising by eye.

```elixir
defmacro __using__(opts) do
  quote do
    alias Bedrock.Internal.ClusterSupervisor
    def fetch_config, do: ClusterSupervisor.fetch_config(__MODULE__)
  end
end
```

### 2. `__MODULE__.Submodule`

Recovery phases chain by naming the next phase relative to the enclosing module:

```elixir
# lib/bedrock/control_plane/director/recovery.ex:343
def run_recovery_attempt(t, context, next_phase_module \\ __MODULE__.TSLValidationPhase)
```

The alias head is not an atom, so a naive `__aliases__` walk drops it and the
entire recovery phase chain looks dead. The analyzer resolves `__MODULE__`
against the enclosing module, in **both** reference and `alias` position -- the
`alias __MODULE__.Types` / `import_types(Types.Foo)` pairing that Absinthe
schemas are built from is the same shape, and dropping it hides a whole
subsystem behind one unresolved name.

### 3. Behaviour impls chosen at runtime

`Bedrock.Encoding` has three impls, `Bedrock.ObjectStorage` has two, and which
one runs is a config atom. Nothing references the impl module statically:

```elixir
backend = ObjectStorage.backend(LocalFilesystem, root: path)
```

The analyzer adds an edge from a behaviour to each of its implementors, so
declaring `@behaviour` keeps an impl alive. **This means an impl of a dead
behaviour is correctly dead, but an impl of a live behaviour is always live** --
including genuinely abandoned ones. Check impls by hand.

### 4. Telemetry handlers

Attached at runtime by name, with a capture that hides the reference:

```elixir
:telemetry.attach_many(handler_id, events, &__MODULE__.handler/4, nil)
```

Whatever calls `Tracing.start/0` is the real root. If that caller is itself only
reachable dynamically, the whole tracing module looks dead.

### 5. Supervision children built from config

`ClusterSupervisor.children_for_capabilities/3` maps capability atoms to modules
through `module_for_capability/1`. That lookup table does name its modules
statically, so these edges survive -- but a child added by config alone, or by
`Module.concat`, would not.

```
lib/mix/bedrock.ex:49            |> Module.concat()
lib/bedrock/service/manifest.ex  worker_name |> String.split(".") |> Module.concat()
```

Anything reachable only through a `Module.concat` is invisible to every static
tool. `manifest.ex` reconstructs worker modules from persisted strings.

### 6. `defprotocol`

A protocol defines a module, but it is not a `defmodule`. Until the analyzer
learned this, a protocol file owned no name in the graph, so no reference to it
could resolve. `lib/bedrock/type_coercion.ex` is the case here: it holds
`Bedrock.ToKeyRange` and `Bedrock.ToKeyspace`, both called from `keyspace.ex`,
`repo.ex` and `directory.ex`, and it was still being reported as public API with
no internal caller. `defimpl` bodies are walked for references but define no
name worth tracking -- an impl lives or dies with its protocol.

### 7. Modules named only in config

A module looked up at runtime from application env is named in `config/*.exs`
and nowhere else. The analyzer parses every discovered project's config and
roots what it finds, so this class is handled without manifest entries. Inert in
this repo, which has no `config/` -- it earns its keep in an application, where
adapters, event handlers and job queues are all wired this way.

### 8. Framework naming conventions

Phoenix `scope "/x", Some.Namespace do ... end` prefixes bare aliases inside the
block with no `alias` line anywhere, and controllers pick their view by name
rather than by reference. The analyzer accumulates `scope` prefixes; the view
convention it cannot see at all. Neither applies to this repo. They are listed
because the analyzer is shared across projects and the code is there.

### 9. Mix tasks

CLI-invoked, never aliased. Anything under a `lib/mix/` directory is rooted
automatically.

## Things that are *not* evidence of life

- **An incoming reference from a test.** Tests are not roots. A module whose
  only callers are its own test and a sibling's test is dead -- that is precisely
  the `ChunkWriter` case.
- **An incoming reference from another dead module.** Deadness is transitive;
  that is the whole reason for computing reachability instead of in-degree.
- **A `@moduledoc` describing planned functionality.** `ChunkWriter`'s moduledoc
  documents a full usage example and cites a future compaction ticket. It is
  still dead. Intent is not a caller.
- **Being mentioned in `guides/`.** Documentation of a dead module is a docs bug,
  not a reference. Delete the prose with the code.

## Sub-file granularity

A test file that exercises both live and dead modules cannot be deleted
wholesale. `test/bedrock/object_storage/chunk_reader_test.exs` tests the live
`ChunkReader` and carries one block for the dead `ChunkWriter`:

```elixir
describe "integration with ChunkWriter" do
```

The analyzer classifies test files by **subject** -- the module its own name
points at (`ChunkWriterTest` -> `ChunkWriter`) -- rather than by everything they
touch, because a test that builds a fixture from a live storage backend is not
thereby testing that backend.

## Calibration case

`Bedrock.ObjectStorage.ChunkWriter` is the reference example. Expected findings:

- module: dead, zero incoming edges
- `chunk_writer_test.exs`: delete outright, subject is dead
- `chunk_reader_test.exs`: surgical edit, one `describe` block
- `ChunkReader` alongside it: **live** (`demux/shard_server.ex`)

If a change to the analyzer breaks any of those four, the change is wrong.

`Bedrock.ToKeyRange` must stay **off** the unused-public-API list: it is the
regression test for `defprotocol` ownership (class 6).
