defmodule Bedrock.Service.Foreman.Supervisor do
  @moduledoc false

  @doc false
  @type foreman_opts :: [
          cluster: module(),
          capabilities: [Bedrock.Cluster.capability()],
          path: Path.t(),
          object_storage: term()
        ]

  @spec child_spec(foreman_opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"
    capabilities = Keyword.get(opts, :capabilities) || raise "Missing :capabilities option"
    path = Keyword.get(opts, :path) || raise "Missing :path option"
    object_storage = Keyword.get(opts, :object_storage) || raise "Missing :object_storage option"

    children = [
      {DynamicSupervisor, name: cluster.otp_name(:worker_supervisor)},
      {Bedrock.Service.Foreman.Server,
       [
         cluster: cluster,
         capabilities: capabilities,
         path: path,
         otp_name: cluster.otp_name(:foreman),
         object_storage: object_storage
       ]}
    ]

    %{
      id: __MODULE__,
      start: {
        Supervisor,
        :start_link,
        [
          children,
          [strategy: :one_for_one]
        ]
      },
      restart: :permanent
    }
  end
end
