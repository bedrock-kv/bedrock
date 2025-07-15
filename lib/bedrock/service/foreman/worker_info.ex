defmodule Bedrock.Service.Foreman.WorkerInfo do
  @type t :: %__MODULE__{}
  @enforce_keys [:id, :path, :health]
  defstruct [
    :id,
    :path,
    :health,
    :manifest,
    :otp_name
  ]

  @spec put_health(t(), term()) :: t()
  def put_health(t, health), do: %{t | health: health}
  @spec put_manifest(t(), term()) :: t()
  def put_manifest(t, manifest), do: %{t | manifest: manifest}
  @spec put_otp_name(t(), atom()) :: t()
  def put_otp_name(t, otp_name), do: %{t | otp_name: otp_name}
end
