defmodule Bedrock.JobQueue.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # JobQueue consumers are started via the user's JobQueue module (e.g., MyApp.JobQueue)
    # which uses Bedrock.JobQueue.Supervisor. No global children needed.
    children = []

    opts = [strategy: :one_for_one, name: Bedrock.JobQueue.ApplicationSupervisor]
    Supervisor.start_link(children, opts)
  end
end
