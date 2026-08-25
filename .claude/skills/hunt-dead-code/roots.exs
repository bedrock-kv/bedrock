# Declared roots for the transitive dead-code analyzer.
#
# A root is code that is reachable from outside this repository -- by a
# downstream user, by the BEAM, by a CLI, or by runtime wiring that no static
# analysis can follow. Everything not transitively reachable from a root is a
# deletion candidate.
#
# THIS FILE IS THE RATCHET. When the analyzer proposes a cluster and the
# verification gate proves it live, the fix is not to tweak the analyzer -- it
# is to add the entry point here with the reason it is reachable. The noise
# floor then drops permanently, and this file accumulates into documentation of
# the dynamic-wiring surface, which is knowledge the codebase records nowhere
# else.
#
# Each root is {path, reason}. Write the reason for someone who does not
# already know why the module is alive.

%{
  # Reached from outside the library. Because Bedrock is a published package,
  # these can never be *proven* dead from inside it -- the analyzer reports
  # uncalled ones as advisory instead of proposing deletion.
  public_api: [
    "lib/bedrock.ex",
    "lib/bedrock/cluster.ex",
    "lib/bedrock/directory.ex",
    "lib/bedrock/durability.ex",
    "lib/bedrock/encoding.ex",
    "lib/bedrock/high_contention_allocator.ex",
    "lib/bedrock/key.ex",
    "lib/bedrock/key_range.ex",
    "lib/bedrock/key_selector.ex",
    "lib/bedrock/keyspace.ex",
    "lib/bedrock/object_storage.ex",
    "lib/bedrock/repo.ex",
    "lib/bedrock/system_keys.ex",
    "lib/bedrock/telemetry.ex",
    "lib/bedrock/type_coercion.ex"
  ],

  # Reachable, but not by any edge the graph can see. Paths may contain globs,
  # so one dynamically-dispatched family is one entry with one reason.
  #
  # Two classes are rooted automatically and never need listing: anything under
  # a `lib/mix/` directory, and any module named in a discovered project's
  # `config/*.exs` (this repo has no config/, so that one is inert here).
  roots: [
    # -- add confirmed false positives here, with the reason --
  ]
}
