defmodule Bedrock.JobQueue.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Job module registry
      {Registry, keys: :duplicate, name: Bedrock.JobQueue.Registry.Default},

      # Dynamic supervisor for consumer instances
      {DynamicSupervisor, name: Bedrock.JobQueue.ConsumerSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: Bedrock.JobQueue.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
