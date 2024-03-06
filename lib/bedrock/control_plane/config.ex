defmodule Bedrock.ControlPlane.Config do
  @type t :: %__MODULE__{}
  defstruct [
    # The current state of the cluster.
    state: :initializing,

    # The parameters that are used to configure the cluster.
    parameters: nil,

    # The policies that are used to configure the cluster.
    policies: nil,

    # The layout of the transaction system.
    transaction_system_layout: nil
  ]

  defmodule Parameters do
    @type t :: %__MODULE__{}
    defstruct [
      # A list of nodes that are participating in the cluster.
      nodes: [],

      # The rate at which the controller is to ping the nodes, expressed in
      # Hertz
      ping_rate_in_hz: 10,

      # The rate at which the system is to retransmit messages, expressed in
      # Hertz.
      retransmission_rate_in_hz: 20,

      # The (minimum) number of nodes that must acknowledge a write before it is
      # considered successful.
      replication_factor: 1,

      # The number of coordinators that are to be made available within the
      # system.
      desired_coordinators: 1,

      # The number of transaction logs that are to be made available within the
      # system. Must be equal to or greater than the replication factor.
      desired_logs: 1,

      # The number of get read version proxies that are to be made available as
      # part of the transaction system.
      desired_get_read_version_proxies: 1,

      # The number of commit proxies that are to be made available as part of
      # the transaction system.
      desired_commit_proxies: 1,

      # The number of transaction resolvers that are to be made available as
      # part of the transaction system.
      desired_transaction_resolvers: 1
    ]
  end

  defmodule Policies do
    defstruct [
      # Should nodes that volunteer to join the cluster be allowed to do so?
      allow_volunteer_nodes_to_join: true
    ]
  end

  defmodule TransactionSystemLayout do
    @type t :: %__MODULE__{}
    defstruct [
      # The full otp name of the cluster controller.
      controller: nil,

      # The full otp name of the cluster sequencer.
      sequencer: nil,

      # The full otp name of the system rate-keeper.
      rate_keeper: nil,

      # The full otp name of the cluster data distributor.
      data_distributor: nil,

      # The full otp names of the get-read-version proxies.
      get_read_version_proxies: [],

      # The full otp names of the commit proxies.
      commit_proxies: [],

      # The full otp names of the transaction resolvers.
      transaction_resolvers: [],

      # A list of logs that are responsible for storing the transactions on
      # their way to the storage teams. Each log contains a list of the tags
      # that it services, and the full otp name of the log worker process that
      # is responsible for the log.
      logs: [],

      # A list of storage teams that are responsible for storing the data within
      # the system. Each team represents a shard of the key space, and contains
      # a list of the storage worker ids that are responsible for the shard.
      storage_teams: [],

      # A list of all of the workers within the system, their types, ids and
      # the otp names used to communicate with them.
      service_directory: []
    ]
  end

  defmodule StorageTeamDescriptor do
    @type t :: %__MODULE__{}
    defstruct [
      # The first key in the range of keys that the team is responsible for.
      start_key: nil,

      # The tag that identifies the team.
      tag: nil,

      # The list of storage workers that are responsible for the team.
      storage_worker_ids: []
    ]
  end

  defmodule LogDescriptor do
    @type t :: %__MODULE__{}
    defstruct [
      # The set of tags that the log services.
      tags: [],

      # The id of the log worker that is responsible for this set of tags.
      log_worker_id: nil
    ]
  end

  defmodule ServiceDescriptor do
    @type t :: %__MODULE__{}
    defstruct [
      # The unique id of the service.
      id: nil,

      # The type of the service.
      type: nil,

      # The otp name of the service.
      otp_name: nil
    ]
  end

  @spec allow_volunteer_nodes_to_join?(t()) :: boolean()
  def allow_volunteer_nodes_to_join?(t), do: t.policies.allow_volunteer_nodes_to_join

  @spec nodes(t()) :: [node()]
  def nodes(t), do: t.parameters.nodes

  @spec ping_rate_in_ms(t()) :: non_neg_integer()
  def ping_rate_in_ms(t), do: div(1000, t.parameters.ping_rate_in_hz)

  @spec sequencer(t()) :: pid() | nil
  def sequencer(t), do: find_singleton_service(t, :sequencer)

  @spec data_distributor(t()) :: pid() | nil
  def data_distributor(t), do: find_singleton_service(t, :data_distributor)

  @spec log_workers(t()) :: [pid()]
  def log_workers(t), do: find_multiple_services(t, :log)

  def service_directory(%__MODULE__{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: service_directory
        }
      }),
      do: service_directory

  @spec find_singleton_service(t(), atom()) :: pid() | nil
  defp find_singleton_service(%__MODULE__{} = t, service_type) do
    t |> service_directory() |> Enum.find(nil, &match?(%{type: ^service_type}, &1.otp_name))
  end

  @spec find_multiple_services(t(), atom()) :: [pid()]
  defp find_multiple_services(%__MODULE__{} = t, service_type) do
    t |> service_directory() |> Enum.filter(&match?(%{type: ^service_type}, &1.otp_name))
  end
end
