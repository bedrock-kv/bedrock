defmodule Bedrock.Service.Foreman.WorkerInfo do
  @moduledoc false

  alias Bedrock.Service.Manifest
  alias Bedrock.Service.Worker

  # A start failure carries whatever explained it, and the set is open:
  # a posix error from the working directory, a validation atom from the
  # manifest, `:timeout` or an exit reason from the start task, or an
  # exception raised by the worker module's own code. Nothing matches on
  # the reason — the verdict only asks whether there IS one — so naming a
  # closed set here would only be a claim we cannot keep.
  @type health ::
          {:ok, Worker.ref()}
          | {:failed_to_start, reason :: term()}
          | :stopped
  @type t :: %__MODULE__{}
  @enforce_keys [:id, :path, :health]

  defstruct [
    :id,
    :path,
    :health,
    :manifest,
    :otp_name,
    :monitor_ref
  ]

  @spec put_health(t(), health()) :: t()
  def put_health(t, health), do: %{t | health: health}

  @spec put_monitor_ref(t(), reference() | nil) :: t()
  def put_monitor_ref(t, monitor_ref), do: %{t | monitor_ref: monitor_ref}

  @spec put_manifest(t(), Manifest.t()) :: t()
  def put_manifest(t, manifest), do: %{t | manifest: manifest}

  @spec put_otp_name(t(), atom()) :: t()
  def put_otp_name(t, otp_name), do: %{t | otp_name: otp_name}
end
