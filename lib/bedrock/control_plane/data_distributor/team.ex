defmodule Bedrock.ControlPlane.DataDistributor.Team do
  defstruct ~w[tag key_range replication_factor log_storage_pairs]a
  @type t :: %__MODULE__{}

  def new(tag, key_range, replication_factor \\ 1) do
    %__MODULE__{
      tag: tag,
      key_range: key_range,
      replication_factor: replication_factor,
      log_storage_pairs: []
    }
  end

  def storage_engines_for_key(_team, _key), do: []
end
