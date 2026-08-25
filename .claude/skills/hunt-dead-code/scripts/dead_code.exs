#!/usr/bin/env elixir
#
# Transitive dead-code analyzer.
#
# "Dead" means: unreachable from the declared roots, where tests are NOT roots.
# A module reachable only from other dead modules and from its own tests is dead,
# however many incoming edges it has. That transitivity is the whole point --
# in-degree counting cannot find a self-referencing cluster that nothing outside
# it reaches.
#
# The reference graph is built from source AST, not from `mix xref`, because
# xref cannot see through `quote`. In a library that exposes `__using__` macros,
# the calls that keep the runtime guts alive are injected into the *caller's*
# module and never appear as an edge here. Building from AST over-approximates
# liveness, which is the direction you want: missing some dead code is cheap,
# deleting live code is not.
#
# Usage:
#   elixir .claude/skills/hunt-dead-code/scripts/dead_code.exs [--json] [--root DIR]

defmodule DeadCode.Source do
  @moduledoc "Parses one source file into the facts the graph needs."

  defstruct [:path, :modules, :aliases, :refs, :behaviours, :functions, :impls]

  def parse(path, root) do
    rel = Path.relative_to(path, root)

    case path |> File.read!() |> Code.string_to_quoted() do
      {:ok, ast} ->
        modules = modules(ast)
        primary = List.first(modules)
        aliases = aliases(ast)

        %__MODULE__{
          path: rel,
          modules: modules,
          aliases: aliases,
          refs: refs(ast, aliases, primary),
          behaviours: behaviours(ast, aliases),
          functions: functions(ast, primary),
          impls: impls(ast)
        }

      {:error, _} ->
        %__MODULE__{path: rel, modules: [], aliases: %{}, refs: [], behaviours: [], functions: [], impls: []}
    end
  end

  defp dotted(parts), do: Enum.join(parts, ".")

  # Every module defined in the file, outermost first. Nested `defmodule`s are
  # qualified against their enclosing module the way the compiler does.
  def modules(ast) do
    {_, {mods, _}} =
      Macro.traverse(
        ast,
        {[], []},
        fn
          {:defmodule, _, [{:__aliases__, _, parts} | _]} = node, {mods, stack} when is_list(parts) ->
            name =
              if stack == [],
                do: dotted(parts),
                else: dotted([List.last(stack) | Enum.map(parts, &to_string/1)])

            {node, {mods ++ [name], stack ++ [name]}}

          node, acc ->
            {node, acc}
        end,
        fn
          {:defmodule, _, _} = node, {mods, stack} ->
            {node, {mods, Enum.drop(stack, -1)}}

          node, acc ->
            {node, acc}
        end
      )

    mods
  end

  # alias Foo.Bar / alias Foo.Bar, as: Baz / alias Foo.{Bar, Baz}
  # Treated as file-global rather than lexically scoped. That over-links, which
  # over-approximates liveness -- the safe direction.
  def aliases(ast) do
    {_, table} =
      Macro.prewalk(ast, %{}, fn
        {:alias, _, [{{:., _, [{:__aliases__, _, base}, :{}]}, _, children}]} = node, acc
        when is_list(base) and is_list(children) ->
          if Enum.all?(base, &is_atom/1) do
            table =
              Enum.reduce(children, acc, fn
                {:__aliases__, _, parts}, a when is_list(parts) ->
                  if Enum.all?(parts, &is_atom/1),
                    do: Map.put(a, to_string(List.last(parts)), dotted(base ++ parts)),
                    else: a

                _, a ->
                  a
              end)

            {node, table}
          else
            {node, acc}
          end

        {:alias, _, [{:__aliases__, _, parts}, opts]} = node, acc when is_list(parts) ->
          if Enum.all?(parts, &is_atom/1) do
            case Keyword.get(List.wrap(opts), :as) do
              {:__aliases__, _, [as]} when is_atom(as) ->
                {node, Map.put(acc, to_string(as), dotted(parts))}

              _ ->
                {node, Map.put(acc, to_string(List.last(parts)), dotted(parts))}
            end
          else
            {node, acc}
          end

        {:alias, _, [{:__aliases__, _, parts}]} = node, acc when is_list(parts) ->
          if Enum.all?(parts, &is_atom/1),
            do: {node, Map.put(acc, to_string(List.last(parts)), dotted(parts))},
            else: {node, acc}

        node, acc ->
          {node, acc}
      end)

    table
  end

  # Every module referenced anywhere -- inside `quote` blocks included, which is
  # exactly what xref misses.
  def refs(ast, aliases, primary) do
    {_, refs} =
      Macro.prewalk(ast, [], fn
        # `__MODULE__.Submodule` -- how recovery phases chain to one another.
        {:__aliases__, _, [{:__MODULE__, _, _} | rest]} = node, acc when is_list(rest) ->
          if primary && Enum.all?(rest, &is_atom/1),
            do: {node, [dotted([primary | Enum.map(rest, &to_string/1)]) | acc]},
            else: {node, acc}

        {:__aliases__, _, parts} = node, acc when is_list(parts) ->
          if Enum.all?(parts, &is_atom/1) do
            full = dotted(parts)
            head = to_string(List.first(parts))

            # A bare `Foo` may be an alias for something longer; record both the
            # literal name and the expansion so either can resolve.
            expanded =
              case Map.get(aliases, head) do
                nil -> []
                base -> [dotted([base | Enum.drop(parts, 1) |> Enum.map(&to_string/1)])]
              end

            {node, [full | expanded] ++ acc}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(refs)
  end

  def behaviours(ast, aliases) do
    {_, bs} =
      Macro.prewalk(ast, [], fn
        {:@, _, [{:behaviour, _, [{:__aliases__, _, parts}]}]} = node, acc when is_list(parts) ->
          if Enum.all?(parts, &is_atom/1) do
            head = to_string(List.first(parts))
            full = dotted(parts)
            expanded = if base = Map.get(aliases, head), do: [base], else: []
            {node, [full | expanded] ++ acc}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(bs)
  end

  @def_kinds [:def, :defmacro, :defguard, :defdelegate]

  # Public function definitions, as {module, name, arity}. Default arguments
  # widen one definition into a range of arities.
  def functions(ast, primary) do
    {_, fns} =
      Macro.prewalk(ast, [], fn
        {kind, _, [head | _]} = node, acc when kind in @def_kinds ->
          case fn_head(head) do
            {name, args} ->
              defaults = Enum.count(args, &match?({:\\, _, _}, &1))
              total = length(args)
              arities = (total - defaults)..total//1
              {node, Enum.map(arities, &{primary, name, &1}) ++ acc}

            nil ->
              {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(fns)
  end

  defp fn_head({:when, _, [inner | _]}), do: fn_head(inner)
  defp fn_head({name, _, args}) when is_atom(name) and is_list(args), do: {name, args}
  defp fn_head({name, _, nil}) when is_atom(name), do: {name, []}
  defp fn_head(_), do: nil

  # Names carrying `@impl` -- behaviour callbacks, dispatched by something else.
  def impls(ast) do
    {_, {names, _}} =
      Macro.prewalk(ast, {[], false}, fn
        {:@, _, [{:impl, _, _}]} = node, {names, _} ->
          {node, {names, true}}

        {kind, _, [head | _]} = node, {names, pending} when kind in @def_kinds ->
          case {fn_head(head), pending} do
            {{name, _}, true} -> {node, {[name | names], false}}
            _ -> {node, {names, pending}}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(names)
  end

  # Every function name invoked, captured, or named as an atom anywhere in the
  # file. Name-only (arity-insensitive) on purpose: this feeds a conservative
  # "is this name mentioned at all" check.
  def called_names(ast) do
    {_, names} =
      Macro.prewalk(ast, [], fn
        {{:., _, [_, name]}, _, _} = node, acc when is_atom(name) ->
          {node, [name | acc]}

        {:&, _, [{:/, _, [{name, _, _}, _]}]} = node, acc when is_atom(name) ->
          {node, [name | acc]}

        {name, _, args} = node, acc when is_atom(name) and is_list(args) ->
          {node, [name | acc]}

        node, acc when is_atom(node) ->
          {node, [node | acc]}

        node, acc ->
          {node, acc}
      end)

    MapSet.new(names)
  end
end

defmodule DeadCode.Graph do
  @moduledoc "Module reference graph and reachability over it."

  def build(sources) do
    owner =
      for s <- sources, m <- s.modules, into: %{} do
        {m, s.path}
      end

    edges =
      for s <- sources,
          r <- s.refs,
          tgt = Map.get(owner, r),
          tgt != nil,
          tgt != s.path,
          uniq: true,
          do: {s.path, tgt}

    # A module implementing a behaviour is kept alive by whatever dispatches
    # that behaviour, so treat the behaviour as pointing back at the impl.
    impl_edges =
      for s <- sources,
          b <- s.behaviours,
          src = Map.get(owner, b),
          src != nil,
          src != s.path,
          uniq: true,
          do: {src, s.path}

    all = edges ++ impl_edges

    %{
      owner: owner,
      adj: Enum.group_by(all, &elem(&1, 0), &elem(&1, 1)),
      rev: Enum.group_by(all, &elem(&1, 1), &elem(&1, 0)),
      nodes: sources |> Enum.map(& &1.path) |> MapSet.new()
    }
  end

  def reachable(graph, roots) do
    Stream.iterate({MapSet.new(roots), roots}, fn {seen, frontier} ->
      next =
        frontier
        |> Enum.flat_map(&Map.get(graph.adj, &1, []))
        |> Enum.reject(&MapSet.member?(seen, &1))
        |> Enum.uniq()

      {MapSet.union(seen, MapSet.new(next)), next}
    end)
    |> Enum.find(fn {_, frontier} -> frontier == [] end)
    |> elem(0)
  end

  # Weakly-connected components among the dead nodes. A cluster is the unit of
  # deletion: its members only make sense together, so they live or die together.
  def clusters(graph, dead) do
    dead_set = MapSet.new(dead)

    neighbours = fn n ->
      (Map.get(graph.adj, n, []) ++ Map.get(graph.rev, n, []))
      |> Enum.filter(&MapSet.member?(dead_set, &1))
    end

    {clusters, _} =
      Enum.reduce(Enum.sort(dead), {[], MapSet.new()}, fn node, {acc, seen} ->
        if MapSet.member?(seen, node) do
          {acc, seen}
        else
          component = flood([node], MapSet.new([node]), neighbours)
          {[Enum.sort(component) | acc], MapSet.union(seen, MapSet.new(component))}
        end
      end)

    Enum.sort_by(clusters, &{-length(&1), List.first(&1)})
  end

  defp flood([], seen, _), do: MapSet.to_list(seen)

  defp flood(frontier, seen, neighbours) do
    next =
      frontier
      |> Enum.flat_map(neighbours)
      |> Enum.reject(&MapSet.member?(seen, &1))
      |> Enum.uniq()

    flood(next, MapSet.union(seen, MapSet.new(next)), neighbours)
  end
end

defmodule DeadCode.Report do
  def line(s), do: IO.puts(s)
  def rule, do: IO.puts(String.duplicate("=", 76))
  def thin, do: IO.puts(String.duplicate("-", 76))
end

defmodule DeadCode.CLI do
  alias DeadCode.Graph
  alias DeadCode.Report
  alias DeadCode.Source

  def main(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: [json: :boolean, root: :string])
    root = Keyword.get(opts, :root, File.cwd!())

    manifest_path = Path.join(root, ".claude/skills/hunt-dead-code/roots.exs")

    manifest =
      if File.exists?(manifest_path) do
        {m, _} = Code.eval_file(manifest_path)
        m
      else
        %{roots: [], public_api: []}
      end

    lib = Path.wildcard(Path.join(root, "lib/**/*.ex"))
    support = Path.wildcard(Path.join(root, "test/support/**/*.ex"))
    tests = Path.wildcard(Path.join(root, "test/**/*_test.exs"))

    lib_sources = Enum.map(lib, &Source.parse(&1, root))
    support_sources = Enum.map(support, &Source.parse(&1, root))
    test_sources = Enum.map(tests, &Source.parse(&1, root))

    graph = Graph.build(lib_sources)

    declared = manifest |> Map.get(:roots, []) |> Enum.map(&elem(&1, 0))
    public_api = Map.get(manifest, :public_api, [])

    mix_tasks = graph.nodes |> Enum.filter(&String.starts_with?(&1, "lib/mix/"))

    roots =
      (declared ++ public_api ++ mix_tasks)
      |> Enum.uniq()
      |> Enum.filter(&MapSet.member?(graph.nodes, &1))

    live = Graph.reachable(graph, roots)
    dead = graph.nodes |> Enum.reject(&MapSet.member?(live, &1)) |> Enum.sort()
    clusters = Graph.clusters(graph, dead)

    dead_modules =
      for s <- lib_sources, s.path in dead, m <- s.modules, into: MapSet.new(), do: m

    test_index = classify_tests(test_sources ++ support_sources, dead_modules, graph.owner)
    unused_public = unused_public_api(graph, public_api, live)
    dead_fns = dead_public_functions(lib_sources, test_sources, support_sources, dead)

    if Keyword.get(opts, :json) do
      emit_json(clusters, test_index, unused_public, dead_fns)
    else
      emit_text(graph, clusters, test_index, unused_public, dead_fns, roots, live)
    end
  end

  # A test file dies wholesale when its *subject* is dead -- the module its own
  # name points at. Judging by "every module it touches" would spare
  # chunk_writer_test.exs, which also touches a live storage backend to build a
  # fixture; that backend is scaffolding, not the thing under test.
  #
  # A file that survives but still references dead modules needs a surgical
  # edit: chunk_reader_test.exs tests a live module and carries one
  # `describe "integration with ChunkWriter"` block that has to come out.
  defp classify_tests(test_sources, dead_modules, owner) do
    for s <- test_sources, reduce: %{fully_dead: [], partial: []} do
      acc ->
        touched =
          s.refs
          |> Enum.filter(&Map.has_key?(owner, &1))
          |> Enum.uniq()

        dead_refs = Enum.filter(touched, &MapSet.member?(dead_modules, &1))
        subject = subject_module(s)

        cond do
          dead_refs == [] ->
            acc

          subject && MapSet.member?(dead_modules, subject) ->
            %{acc | fully_dead: [{s.path, subject} | acc.fully_dead]}

          # No identifiable subject, but everything it touches is dead.
          subject == nil and touched == dead_refs ->
            %{acc | fully_dead: [{s.path, List.first(dead_refs)} | acc.fully_dead]}

          true ->
            %{acc | partial: [{s.path, dead_refs} | acc.partial]}
        end
    end
  end

  # Bedrock.ObjectStorage.ChunkWriterTest -> Bedrock.ObjectStorage.ChunkWriter
  defp subject_module(%{modules: []}), do: nil

  defp subject_module(%{modules: [primary | _]}) do
    case String.replace_suffix(primary, "Test", "") do
      ^primary -> nil
      "" -> nil
      stripped -> stripped
    end
  end

  # Public modules can never be *proven* dead from inside the library -- a
  # downstream user may call them. Report the ones with no internal caller as
  # advisory, for human judgement, and keep them out of the deletion set.
  defp unused_public_api(graph, public_api, live) do
    public_api
    |> Enum.filter(&MapSet.member?(graph.nodes, &1))
    |> Enum.filter(fn path ->
      callers =
        graph
        |> Map.get(:rev, %{})
        |> Map.get(path, [])
        |> Enum.filter(&MapSet.member?(live, &1))

      callers == []
    end)
    |> Enum.sort()
  end

  @otp_callbacks ~w(
    init handle_call handle_cast handle_info handle_continue terminate code_change
    child_spec start_link start format_status run handle_event callback_mode
  )a

  # Advisory only, and split into two tiers because they warrant different
  # confidence. Behaviour and OTP callbacks are excluded throughout: something
  # dispatches them by name.
  #
  #   :never  -- the name appears in no other file at all. Strongest signal.
  #   :tests  -- the name appears only in test files. Dead by the definition
  #              this tool works to (reachable only from its own tests), but
  #              also the shape of a helper kept deliberately for testing, so
  #              it needs a human read rather than a blanket delete.
  defp dead_public_functions(lib_sources, test_sources, support_sources, dead_paths) do
    dead_set = MapSet.new(dead_paths)
    live_sources = Enum.reject(lib_sources, &MapSet.member?(dead_set, &1.path))

    prod_mentions = mention_index(lib_sources)
    test_mentions = mention_index(test_sources ++ support_sources)

    candidates =
      for s <- live_sources,
          {mod, name, arity} <- s.functions,
          mod != nil,
          name not in @otp_callbacks,
          name not in s.impls,
          not String.starts_with?(to_string(name), "__"),
          not mentioned_elsewhere?(prod_mentions, s.path, name) do
        tier = if mentioned_elsewhere?(test_mentions, s.path, name), do: :tests, else: :never
        {tier, s.path, "#{mod}.#{name}/#{arity}"}
      end
      |> Enum.uniq()

    for tier <- [:never, :tests], into: %{} do
      grouped =
        candidates
        |> Enum.filter(&(elem(&1, 0) == tier))
        |> Enum.group_by(&elem(&1, 1), &elem(&1, 2))

      {tier, grouped}
    end
  end

  defp mention_index(sources) do
    for s <- sources, into: %{} do
      ast =
        case s.path |> File.read!() |> Code.string_to_quoted() do
          {:ok, parsed} -> parsed
          _ -> {:__block__, [], []}
        end

      {s.path, Source.called_names(ast)}
    end
  end

  defp mentioned_elsewhere?(mentions, own_path, name) do
    Enum.any?(mentions, fn {path, names} ->
      path != own_path and MapSet.member?(names, name)
    end)
  end

  defp emit_text(graph, clusters, tests, unused_public, dead_fns, roots, live) do
    Report.rule()
    Report.line("TRANSITIVE DEAD CODE")
    Report.rule()

    Report.line(
      "modules: #{MapSet.size(graph.nodes)}   roots: #{length(roots)}   " <>
        "live: #{MapSet.size(live)}   dead: #{MapSet.size(graph.nodes) - MapSet.size(live)}"
    )

    Report.line("")
    Report.line("## Dead clusters (#{length(clusters)}) -- each is one deletion unit")
    Report.thin()

    if clusters == [] do
      Report.line("  none")
    else
      clusters
      |> Enum.with_index(1)
      |> Enum.each(fn {cluster, i} ->
        Report.line("")
        Report.line("[#{i}] #{length(cluster)} module(s)")
        Enum.each(cluster, &Report.line("      #{&1}"))
      end)
    end

    Report.line("")
    Report.line("## Tests to delete outright (#{length(tests.fully_dead)})")
    Report.thin()

    if tests.fully_dead == [] do
      Report.line("  none")
    else
      tests.fully_dead
      |> Enum.sort()
      |> Enum.each(fn {p, subject} -> Report.line("  #{p}  (subject: #{subject})") end)
    end

    Report.line("")
    Report.line("## Tests needing surgical edits (#{length(tests.partial)})")
    Report.line("   (exercise both live and dead modules -- remove only the dead parts)")
    Report.thin()

    if tests.partial == [] do
      Report.line("  none")
    else
      tests.partial
      |> Enum.sort()
      |> Enum.each(fn {p, hits} ->
        Report.line("  #{p}")
        Enum.each(hits, &Report.line("      dead ref: #{&1}"))
      end)
    end

    Report.line("")
    Report.line("## Unused public API (#{length(unused_public)}) -- ADVISORY, human judgement")
    Report.line("   Cannot be proven dead from inside a library; downstream users may call these.")
    Report.thin()

    if unused_public == [],
      do: Report.line("  none"),
      else: Enum.each(unused_public, &Report.line("  #{&1}"))

    emit_fn_tier(
      dead_fns[:never] || %{},
      "Public functions referenced nowhere at all",
      "   Not called, captured, or named as an atom in any lib or test file."
    )

    emit_fn_tier(
      dead_fns[:tests] || %{},
      "Public functions referenced only by tests",
      "   Dead by this tool's definition, but also the shape of a helper kept\n" <>
        "   deliberately for testing. Read each one before removing."
    )

    Report.line("")
    Report.rule()
    Report.line("Nothing here is proven dead until the verification gate passes.")
    Report.line("See SKILL.md -- delete the cluster, then run the gate.")
    Report.rule()
  end

  defp emit_fn_tier(grouped, title, note) do
    total = grouped |> Map.values() |> List.flatten() |> length()
    Report.line("")
    Report.line("## #{title} (#{total}) -- ADVISORY")
    Report.line(note)
    Report.line("   Dynamic dispatch, protocol impls, and macro-generated calls all defeat")
    Report.line("   this check. Verify before removing.")
    Report.thin()

    if map_size(grouped) == 0 do
      Report.line("  none")
    else
      grouped
      |> Enum.sort()
      |> Enum.each(fn {path, fns} ->
        Report.line("  #{path}")
        fns |> Enum.sort() |> Enum.each(&Report.line("      #{&1}"))
      end)
    end
  end

  # Uses the stdlib JSON module, so the script needs no project deps loaded and
  # runs under plain `elixir`. JSON arrived in Elixir 1.18; mix.exs still allows
  # 1.17, so say so plainly rather than failing with UndefinedFunctionError.
  defp emit_json(clusters, tests, unused_public, dead_fns) do
    if Code.ensure_loaded?(JSON) do
      %{
        "clusters" => clusters,
        "tests_to_delete" =>
          Enum.map(tests.fully_dead, fn {p, s} -> %{"path" => p, "subject" => s} end),
        "tests_to_edit" =>
          Enum.map(tests.partial, fn {p, h} -> %{"path" => p, "dead_refs" => h} end),
        "unused_public_api" => unused_public,
        "unreferenced_functions" => dead_fns[:never] || %{},
        "test_only_functions" => dead_fns[:tests] || %{}
      }
      |> JSON.encode!()
      |> IO.puts()
    else
      IO.puts(:stderr, "--json needs Elixir 1.18+ (stdlib JSON). Re-run without it for the text report.")
      System.halt(1)
    end
  end
end

DeadCode.CLI.main(System.argv())
