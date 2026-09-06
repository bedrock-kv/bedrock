defmodule Bedrock.ControlPlane.Exclusion do
  @moduledoc """
  Whether a set of nodes can be taken out of service without stranding
  data recovery still needs.

  This is FDB's `checkForExcludingServersTxActor`
  (`ManagementAPI.actor.cpp:2393-2408`), the log half: it reads the
  keyspace log record and refuses if any named log — CURRENT generation
  or OLD — sits on a node being excluded. FDB refuses on the same two
  loops over the two vectors in `logsKey`, and for the same reason: a
  machine that still holds a log the next recovery would copy from is not
  safe to remove, however few shards it serves right now.

  Safety is judged against ADDRESSES and GENERATIONS, which is why the
  record it reads is keyed that way. A tag list — which shards a log
  serves — cannot answer the question at all: a survivor recovery has not
  finished with serves no shard in the current epoch and would read as
  idle.

  ## What it does not answer

  Only the log half. FDB's check also walks `serverList` and refuses a
  storage server's address, but there the refusal is provisional — the
  data distributor moves the shards off and the answer becomes yes.
  Bedrock has no such movement yet (elastic placement is bedrock-q67.46),
  so a materializer loop here would refuse forever with no path to
  safety, which is not the answer FDB's has. It is added with the
  movement, not before.

  There is no operator command wired to this yet; exclusion as a feature
  (the `\\xff/conf/excluded/` half, `SystemData.cpp:1021`) is not built.
  This is the reader the log record ships with, per the writer-and-reader
  rule.
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
  Reads the log record through `range_read_fn` and judges whether every
  node in `nodes` can be excluded.

  Nodes are matched as strings, the form the family stores — the keyspace
  never carries atoms, and neither does this.

  Answers `:safe` only on evidence: a read or decode failure is returned
  as an error, never flattened into a verdict, because "the record could
  not be read" and "the record names nobody here" must not look alike to
  an operator about to power a machine down.
  """
  @spec check(Reader.range_read_fn(), [String.t()]) ::
          :safe
          | {:unsafe, [blocker()]}
          | {:error, {:log_locations_query_failed, term()} | {:invalid_log_entry, Bedrock.key()}}
  def check(range_read_fn, nodes) do
    excluded = MapSet.new(nodes)

    with {:ok, entries} <-
           Reader.read_family(range_read_fn, SystemKeys.logs_prefix(), :log_locations_query_failed),
         {:ok, locations} <- Reader.decode_log_locations(entries) do
      case blockers(locations, excluded) do
        [] -> :safe
        blockers -> {:unsafe, blockers}
      end
    end
  end

  defp blockers(locations, excluded) do
    for generation <- [:current, :old],
        {log_id, node} <- Enum.sort(locations[generation]),
        MapSet.member?(excluded, node),
        do: {generation, log_id, node}
  end
end
