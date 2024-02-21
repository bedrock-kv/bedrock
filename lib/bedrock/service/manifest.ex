defmodule Bedrock.Service.Manifest do
  @derive Jason.Encoder
  defstruct ~w[cluster id worker]a
  @type t :: %__MODULE__{}

  @spec new(cluster :: String.t(), id :: String.t(), worker :: module()) :: t()
  def new(cluster, id, worker) do
    %__MODULE__{
      cluster: cluster,
      id: id,
      worker: worker
    }
  end

  @spec write_to_file(manifest :: t(), path_to_manifest :: String.t()) :: :ok | {:error, term()}
  def write_to_file(manifest, path_to_manifest) do
    %{
      cluster: manifest.cluster,
      id: manifest.id,
      worker: manifest.worker |> Module.split() |> Enum.join(".")
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
         {:ok, worker} <- worker_from_json(json["worker"]) do
      {:ok,
       %__MODULE__{
         cluster: cluster,
         id: id,
         worker: worker
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

  @spec worker_from_json(binary()) ::
          {:ok, module()}
          | {:error,
             :worker_module_is_invalid
             | :worker_module_does_not_exist
             | :worker_module_failed_to_load
             | :malformed_worker_name
             | :invalid_worker_name
             | :worker_module_does_not_implement_behaviour}
  defp worker_from_json(worker_name) when is_binary(worker_name) do
    with {:ok, worker_module} <- parse_worker_name(worker_name),
         {:module, worker} <- Code.ensure_loaded(worker_module),
         :ok <- check_module_is_storage_worker(worker) do
      {:ok, worker}
    else
      {:error, :badfile} -> {:error, :worker_module_is_invalid}
      {:error, :nofile} -> {:error, :worker_module_does_not_exist}
      {:error, :on_load_failure} -> {:error, :worker_module_failed_to_load}
      {:error, _reason} = error -> error
    end
  end

  defp worker_from_json(_),
    do: {:error, :invalid_worker_name}

  @spec parse_worker_name(String.t()) :: {:ok, module()} | {:error, :malformed_worker_name}
  defp parse_worker_name(worker_name) do
    worker_name
    |> String.split(".")
    |> case do
      [] -> {:error, :malformed_worker_name}
      components -> {:ok, Module.concat(components)}
    end
  end

  defp check_module_is_storage_worker(worker) do
    if :attributes
       |> worker.module_info()
       |> Enum.member?({:behaviour, [Bedrock.Worker]}) do
      :ok
    else
      {:error, :worker_module_does_not_implement_behaviour}
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
