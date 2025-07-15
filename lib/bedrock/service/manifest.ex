defmodule Bedrock.Service.Manifest do
  alias Bedrock.Cluster

  @derive Jason.Encoder
  @type t :: %__MODULE__{
          cluster: Cluster.name(),
          id: String.t(),
          worker: module(),
          params: map()
        }
  defstruct [:cluster, :id, :worker, :params]

  @spec new(Cluster.name(), id :: String.t(), worker :: module(), params :: map()) :: t()
  def new(cluster, id, worker, params \\ %{}) do
    %__MODULE__{
      cluster: cluster,
      id: id,
      worker: worker,
      params: params
    }
  end

  @spec write_to_file(manifest :: t(), path_to_manifest :: String.t()) ::
          :ok | {:error, File.posix()}
  def write_to_file(manifest, path_to_manifest) do
    {:ok, json} =
      %{
        cluster: manifest.cluster,
        id: manifest.id,
        worker: manifest.worker |> Module.split() |> Enum.join("."),
        params: manifest.params
      }
      |> Jason.encode()

    path_to_manifest |> write_file_contents(json)
  end

  @spec write_file_contents(String.t(), String.t()) :: :ok | {:error, File.posix()}
  defp write_file_contents(path_to_manifest, json),
    do: File.write(path_to_manifest, json)

  @spec load_from_file(path_to_manifest :: String.t()) ::
          {:ok, t()}
          | {:error,
             :manifest_does_not_exist
             | :manifest_is_invalid
             | :manifest_is_not_a_dictionary
             | :worker_module_is_invalid
             | :worker_module_does_not_exist
             | :worker_module_failed_to_load
             | :invalid_cluster_id
             | :invalid_cluster_name
             | :invalid_worker_name
             | :worker_module_does_not_implement_behaviour
             | :invalid_params}
  def load_from_file(path_to_manifest) do
    with {:ok, file_contents} <- path_to_manifest |> load_file_contents(),
         {:ok, json} <- file_contents |> Jason.decode(),
         true <- is_map(json) || {:error, :manifest_is_not_a_dictionary},
         {:ok, cluster} <- cluster_from_json(json["cluster"]),
         {:ok, id} <- id_from_json(json["id"]),
         {:ok, worker} <- worker_from_json(json["worker"]),
         {:ok, params} <- params_from_json(json["params"]) do
      {:ok, new(cluster, id, worker, params)}
    else
      {:error, %Jason.DecodeError{}} ->
        {:error, :manifest_is_invalid}

      {:error, _reason} = error ->
        error
    end
  end

  @spec load_file_contents(String.t()) :: {:ok, String.t()} | {:error, :manifest_does_not_exist}
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
             | :invalid_cluster_id
             | :invalid_cluster_name
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

  @spec parse_worker_name(String.t()) :: {:ok, module()} | {:error, :invalid_worker_name}
  defp parse_worker_name(worker_name),
    do: {:ok, worker_name |> String.split(".") |> Module.concat()}

  @spec check_module_is_storage_worker(module()) ::
          :ok | {:error, :worker_module_does_not_implement_behaviour}
  defp check_module_is_storage_worker(worker) do
    if :attributes
       |> worker.module_info()
       |> Enum.member?({:behaviour, [Bedrock.Service.WorkerBehaviour]}) do
      :ok
    else
      {:error, :worker_module_does_not_implement_behaviour}
    end
  end

  @spec params_from_json(term()) :: {:ok, map()} | {:error, :invalid_params}
  def params_from_json(nil), do: {:ok, %{}}
  def params_from_json(params) when is_map(params), do: {:ok, params}
  def params_from_json(_), do: {:error, :invalid_params}

  @spec id_from_json(value :: binary()) ::
          {:ok, id :: String.t()} | {:error, :invalid_cluster_id}
  defp id_from_json(id) when is_binary(id), do: {:ok, id}
  defp id_from_json(_), do: {:error, :invalid_cluster_id}

  @spec cluster_from_json(value :: binary()) ::
          {:ok, cluster_name :: String.t()} | {:error, :invalid_cluster_name}
  defp cluster_from_json(cluster) when is_binary(cluster), do: {:ok, cluster}
  defp cluster_from_json(_), do: {:error, :invalid_cluster_name}
end
