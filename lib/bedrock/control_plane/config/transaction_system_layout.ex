defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout do
  @moduledoc """
  A `TransactionSystemLayout` is a data structure that describes the layout of
  the transaction system within the cluster.
  """

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Worker

  @typedoc """
  The transaction system's runtime wiring, published once per recovery -
  FDB's ClientDBInfo/ServerDBInfo analogue. Shard topology deliberately
  does NOT ride here: the shard map lives in the `\\xFF/system` keyspace
  and is served to clients by commit proxies (bedrock-q67.9).

  ## Fields
    - `epoch` - The recovery epoch this wiring belongs to.
    - `sequencer` - The pid of the cluster sequencer (read versions).
    - `proxies` - The pids of the commit proxies (commits, routing fetches).
    - `resolvers` - Resolver descriptors, consumed at proxy unlock.
    - `logs` - Log descriptors: each log's id and the tags it services.
    - `services` - Every worker the layout references (types, ids, refs);
       the Foreman retires hosted workers absent from this map.
  """
  @type process_ref :: pid() | nil
  @type proxy_list :: [pid()]
  @type resolver_list :: [ResolverDescriptor.t()]
  @type log_map :: %{Log.id() => LogDescriptor.t()}
  @type service_map :: %{Worker.id() => ServiceDescriptor.t()}

  @type t :: %{
          required(:epoch) => non_neg_integer(),
          required(:sequencer) => process_ref(),
          required(:proxies) => proxy_list(),
          required(:resolvers) => resolver_list(),
          required(:logs) => log_map(),
          required(:services) => service_map()
        }

  @doc """
  Inverts a `%{tag => materializer pid}` assignment through a services map
  into the string-encoded refs the `materializers/` keyspace family
  carries: `%{tag => {worker_id, node}}`. Both the persistence phase (the
  family's writer) and the routing snapshot (the recover_from seed) derive
  from this, so the seed and the keyspace cannot disagree.

  A pid without a matching materializer service record is skipped - the
  family only names workers the layout actually references.
  """
  @spec materializer_refs(%{Bedrock.range_tag() => pid()} | nil, service_map()) ::
          %{Bedrock.range_tag() => {String.t(), String.t()}}
  def materializer_refs(shard_materializers, services) do
    shard_materializers
    |> Kernel.||(%{})
    |> Enum.flat_map(fn
      {tag, pid} when is_pid(pid) ->
        case Enum.find(services, &match?({_, %{kind: :materializer, status: {:up, ^pid}}}, &1)) do
          {worker_id, _descriptor} -> [{tag, {worker_id, Atom.to_string(node(pid))}}]
          nil -> []
        end

      _ ->
        []
    end)
    |> Map.new()
  end
end
