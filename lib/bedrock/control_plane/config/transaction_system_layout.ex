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

  No membership map rides here (FDB's ServerDBInfo carries no storage
  membership either): logs self-check against the epoch-constant log
  set, materializers rejoin-validate against the committed keyspace
  through a commit proxy, and director-internal readers consume the
  recovery attempt's transaction_services. Nothing O(workers) may ever
  be added to this broadcast.
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
          required(:logs) => log_map()
        }
end
