defmodule Bedrock.DataPlane.Storage.Basalt.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage.Basalt.Database
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

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

  @spec update_mode(t(), :locked | :running) :: t()
  def update_mode(t, mode),
    do: %{t | mode: mode}

  @spec update_director_and_epoch(t(), Director.ref() | nil, Bedrock.epoch() | nil) :: t()
  def update_director_and_epoch(t, director, epoch),
    do: %{t | director: director, epoch: epoch}

  @spec reset_puller(t()) :: t()
  def reset_puller(t),
    do: %{t | pull_task: nil}

  @spec put_puller(t(), Task.t()) :: t()
  def put_puller(t, pull_task),
    do: %{t | pull_task: pull_task}
end
