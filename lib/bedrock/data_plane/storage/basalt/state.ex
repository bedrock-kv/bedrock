defmodule Bedrock.DataPlane.Storage.Basalt.State do
  alias Bedrock.DataPlane.Storage.Basalt.Database
  alias Bedrock.Service.Worker
  alias Bedrock.Service.Foreman
  alias Bedrock.ControlPlane.Director

  @type t :: %__MODULE__{
          otp_name: atom(),
          path: Path.t(),
          foreman: Foreman.ref(),
          id: Worker.id(),
          database: Database.t(),
          pull_task: Task.t() | nil,
          epoch: Bedrock.epoch() | nil,
          director: Director.ref() | nil,
          mode: :locked | :running
        }
  defstruct otp_name: nil,
            path: nil,
            foreman: nil,
            id: nil,
            database: nil,
            pull_task: nil,
            epoch: nil,
            director: nil,
            mode: :locked

  def update_mode(t, mode),
    do: %{t | mode: mode}

  def update_director_and_epoch(t, director, epoch),
    do: %{t | director: director, epoch: epoch}

  def reset_puller(t),
    do: %{t | pull_task: nil}

  def put_puller(t, pull_task),
    do: %{t | pull_task: pull_task}
end
