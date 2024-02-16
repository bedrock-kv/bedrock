defmodule Bedrock.Engine.Manifest do
  @derive Jason.Encoder
  defstruct ~w[cluster id engine]a
  @type t :: %__MODULE__{}

  @spec new(cluster :: String.t(), id :: String.t(), engine :: module()) :: t()
  def new(cluster, id, engine) do
    %__MODULE__{
      cluster: cluster,
      id: id,
      engine: engine
    }
  end

  @spec write_to_file(manifest :: t(), path_to_manifest :: String.t()) :: :ok | {:error, term()}
  def write_to_file(manifest, path_to_manifest) do
    %{
      cluster: manifest.cluster,
      id: manifest.id,
      engine: manifest.engine |> Module.split() |> Enum.join(".")
    }
    |> Jason.encode()
    |> case do
      {:ok, json} -> path_to_manifest |> write_file_contents(json)
      {:error, _reason} = error -> error
    end
  end

  defp write_file_contents(path_to_manifest, json),
    do: File.write(path_to_manifest, json)

  @spec load_from_file(path_to_manifest :: String.t()) :: {:ok, t()} | {:error, term()}
  def load_from_file(path_to_manifest) do
    with {:ok, file_contents} <- path_to_manifest |> load_file_contents(),
         {:ok, json} <- file_contents |> Jason.decode(),
         true <- is_map(json) || {:error, :manifest_is_not_a_dictionary},
         {:ok, cluster} <- cluster_from_json(json["cluster"]),
         {:ok, id} <- id_from_json(json["id"]),
         {:ok, engine} <- engine_from_json(json["engine"]) do
      {:ok,
       %__MODULE__{
         cluster: cluster,
         id: id,
         engine: engine
       }}
    end
  end

  defp load_file_contents(path) do
    File.read(path)
    |> case do
      {:ok, _file_contents} = result -> result
      {:error, :enoent} -> {:error, :manifest_does_not_exist}
    end
  end

  @spec engine_from_json(binary()) ::
          {:ok, module()}
          | {:error,
             :engine_module_is_invalid
             | :engine_module_does_not_exist
             | :engine_module_failed_to_load
             | :malformed_engine_name
             | :invalid_engine_name
             | :engine_module_does_not_implement_behaviour}
  defp engine_from_json(engine_name) when is_binary(engine_name) do
    with {:ok, engine_module} <- parse_engine_name(engine_name),
         {:module, engine} <- Code.ensure_loaded(engine_module),
         :ok <- check_module_is_storage_engine(engine) do
      {:ok, engine}
    else
      {:error, :badfile} -> {:error, :engine_module_is_invalid}
      {:error, :nofile} -> {:error, :engine_module_does_not_exist}
      {:error, :on_load_failure} -> {:error, :engine_module_failed_to_load}
      {:error, _reason} = error -> error
    end
  end

  defp engine_from_json(_),
    do: {:error, :invalid_engine_name}

  @spec parse_engine_name(String.t()) :: {:ok, module()} | {:error, :malformed_engine_name}
  defp parse_engine_name(engine_name) do
    engine_name
    |> String.split(".")
    |> case do
      [] -> {:error, :malformed_engine_name}
      components -> {:ok, Module.concat(components)}
    end
  end

  defp check_module_is_storage_engine(engine) do
    if :attributes
       |> engine.module_info()
       |> Enum.member?({:behaviour, [Bedrock.Engine]}) do
      :ok
    else
      {:error, :engine_module_does_not_implement_behaviour}
    end
  end

  @spec id_from_json(value :: binary()) ::
          {:ok, id :: String.t()} | {:error, :malformed_cluster_id}
  defp id_from_json(id) when is_binary(id), do: {:ok, id}
  defp id_from_json(_), do: {:error, :malformed_cluster_id}

  @spec cluster_from_json(value :: binary()) ::
          {:ok, cluster_name :: String.t()} | {:error, :malformed_cluster_name}
  defp cluster_from_json(cluster) when is_binary(cluster), do: {:ok, cluster}
  defp cluster_from_json(_), do: {:error, :malformed_cluster_name}
end
