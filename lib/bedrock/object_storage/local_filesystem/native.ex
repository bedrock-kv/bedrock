defmodule Bedrock.ObjectStorage.LocalFilesystem.Native do
  @moduledoc false
  @on_load :load

  @spec load() :: :ok | {:error, term()}
  def load do
    :bedrock
    |> :code.priv_dir()
    |> :filename.join(~c"local_filesystem_mutation")
    |> :erlang.load_nif(0)
  end

  @spec mutate(:put | :create | :cas | :delete, binary(), binary(), binary(), binary()) :: :ok | {:error, atom()}
  def mutate(_operation, _directory, _name, _scratch, _expected), do: :erlang.nif_error(:nif_not_loaded)
end
