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

  def put_health(t, health), do: %{t | health: health}
  def put_manifest(t, manifest), do: %{t | manifest: manifest}
  def put_otp_name(t, otp_name), do: %{t | otp_name: otp_name}
end
