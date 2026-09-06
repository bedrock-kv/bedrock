defmodule Bedrock.ControlPlane.Exclusion do
  @moduledoc """
  The log half of exclusion safety: which logs stand in the way of taking
  a set of nodes out of service.

  This is one of the two loops in FDB's `checkForExcludingServersTxActor`
  (`ManagementAPI.actor.cpp:2393-2408`), which reads `logsKey` and fails
  the safety check for a log whose address is excluded — walking the
  CURRENT generation and the OLD one alike. A machine that still holds a
  log the next recovery would copy from is not safe to remove, however
  few shards it serves right now.

  Safety is judged against ADDRESSES and GENERATIONS, which is why the
  record it reads is keyed that way. A tag list — which shards a log
  serves — cannot answer the question at all: a survivor recovery has not
  finished with is named by no current shard, so it would read as idle.

  ## This is half an answer, and the name says so

  `check_logs/2` returns `:no_log_blockers`, never `:safe`. FDB's check
  also walks `serverListKeys` and refuses an excluded storage server's
  address — unconditionally, and BEFORE it reads `logsKey` at all. We have
  no equivalent: the `materializers/` family names members, but with no
  data movement to relocate a shard off a machine (elastic placement is
  bedrock-q67.46) the answer for the last member of a tag would be a
  permanent refusal with no remedy but "add a replica first". That half
  arrives with the movement. Until it does, a caller that reads
  `:no_log_blockers` as "safe to power down" can still strand a shard's
  only materializer, so the verdict is named for exactly what was checked.

  One FDB refusal has no analogue here by construction: FDB also fails on
  a log whose recorded address is the empty `NetworkAddress`, meaning the
  interface was not present when the record was written. Our entries carry
  the node a locked log was reached at, so there is no "named but
  addressless" state to represent.

  There is no operator command wired to this yet — exclusion as a feature
  (FDB's `\\xff/conf/excluded/`, `SystemData.cpp:1021`) is not built. This
  is the reader the log record ships with, per the writer-and-reader rule.
  """

  alias Bedrock.Service.Worker
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Reader

  @typedoc """
  A named log standing in the way of an exclusion: which generation named
  it, which log it is, and the node it sits on.
  """
  @type blocker :: {:current | :old, Worker.id(), node_name :: String.t()}

  @doc """
  Reads the log record through `range_read_fn` and reports every named log
  sitting on one of `nodes`.

  Nodes are matched as strings, the form the family stores — the keyspace
  never carries atoms, and neither does this.

  Reports `:no_log_blockers` only on evidence: a read or decode failure is
  returned as an error, never flattened into a verdict, because "the
  record could not be read" and "the record names nobody here" must not
  look alike to an operator about to power a machine down.
  """
  @spec check_logs(Reader.range_read_fn(), [String.t()]) ::
          :no_log_blockers
          | {:unsafe, [blocker()]}
          | {:error, {:log_locations_query_failed, term()} | {:invalid_log_entry, Bedrock.key()}}
  def check_logs(range_read_fn, nodes) when is_list(nodes) do
    excluded = MapSet.new(nodes, &node_name!/1)

    with {:ok, entries} <-
           Reader.read_family(range_read_fn, SystemKeys.logs_prefix(), :log_locations_query_failed),
         {:ok, locations} <- Reader.decode_log_locations(entries) do
      case blockers(locations, excluded) do
        [] -> :no_log_blockers
        blockers -> {:unsafe, blockers}
      end
    end
  end

  # An atom node is the natural Elixir shape and the one the recovery
  # phases carry until they stringify. Matched against a family of strings
  # it would silently match nothing, and the miss would surface as the
  # DANGEROUS verdict — the one thing this module refuses to reach without
  # evidence.
  defp node_name!(node) when is_binary(node), do: node

  defp node_name!(other) do
    raise ArgumentError, "nodes are matched as strings, the form the log record stores: #{inspect(other)}"
  end

  defp blockers(locations, excluded) do
    for generation <- [:current, :old],
        {log_id, node} <- Enum.sort(locations[generation]),
        MapSet.member?(excluded, node),
        do: {generation, log_id, node}
  end
end
