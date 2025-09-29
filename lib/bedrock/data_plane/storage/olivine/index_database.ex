defmodule Bedrock.DataPlane.Storage.Olivine.IndexDatabase do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Version

  @opaque t :: %__MODULE__{
            dets_storage: :dets.tab_name(),
            durable_version: Bedrock.version()
          }

  defstruct [
    :dets_storage,
    :durable_version
  ]

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path) do
    storage_opts = [
      {:type, :set},
      {:access, :read_write},
      {:auto_save, :infinity},
      {:estimated_no_objects, 1_000_000}
    ]

    case :dets.open_file(otp_name, [{:file, String.to_charlist(file_path <> ".idx")} | storage_opts]) do
      {:ok, dets_table} ->
        durable_version =
          case load_durable_version(%{dets_storage: dets_table}) do
            {:ok, version} -> version
            {:error, :not_found} -> Version.zero()
          end

        {:ok,
         %__MODULE__{
           dets_storage: dets_table,
           durable_version: durable_version
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec close(t()) :: :ok
  def close(index_db) do
    try do
      :dets.sync(index_db.dets_storage)
    catch
      _, _ -> :ok
    end

    try do
      :dets.close(index_db.dets_storage)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @spec store_page(t(), page_id :: Page.id(), page_tuple :: {Page.t(), Page.id()}) :: :ok | {:error, term()}
  def store_page(index_db, page_id, {page, next_id}) do
    case :dets.insert(index_db.dets_storage, {page_id, {page, next_id}}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_page(t(), page_id :: Page.id()) :: {:ok, {binary(), Page.id()}} | {:error, :not_found}
  def load_page(index_db, page_id) do
    case :dets.lookup(index_db.dets_storage, page_id) do
      [{^page_id, {page_binary, next_id}}] -> {:ok, {page_binary, next_id}}
      [] -> {:error, :not_found}
    end
  end

  @spec store_durable_version(t(), version :: Bedrock.version()) :: t()
  def store_durable_version(index_db, version), do: %{index_db | durable_version: version}

  @spec durable_version(t()) :: Bedrock.version()
  def durable_version(index_db), do: index_db.durable_version

  @spec load_durable_version(t() | %{dets_storage: :dets.tab_name()}) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_durable_version(%{dets_storage: dets_storage}) do
    case :dets.lookup(dets_storage, :durable_version) do
      [{:durable_version, version}] -> {:ok, version}
      [] -> {:error, :not_found}
    end
  end

  @spec flush(
          t(),
          new_durable_version :: Bedrock.version(),
          collected_pages :: [%{Page.id() => {Page.t(), Page.id()}}]
        ) :: :ok
  def flush(index_db, new_durable_version, collected_pages) do
    pages_map =
      Enum.reduce(collected_pages, %{}, fn modified_pages, acc ->
        Map.merge(modified_pages, acc, fn _page_id, new_page, _old_page -> new_page end)
      end)

    dets_tx = [
      {:durable_version, new_durable_version}
      | Map.to_list(pages_map)
    ]

    :dets.insert(index_db.dets_storage, dets_tx)
  end

  @spec sync(t()) :: :ok
  def sync(index_db) do
    :dets.sync(index_db.dets_storage)
    :ok
  catch
    _, _ -> :ok
  end

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) :: any() | :undefined
  def info(index_db, stat) do
    case stat do
      :n_keys ->
        :dets.info(index_db.dets_storage, :no_objects) || 0

      :size_in_bytes ->
        :dets.info(index_db.dets_storage, :file_size) || 0

      :utilization ->
        calculate_utilization(index_db.dets_storage)

      :key_ranges ->
        []

      _ ->
        :undefined
    end
  end

  defp calculate_utilization(dets_storage) do
    case :dets.info(dets_storage, :no_objects) do
      nil -> 0.0
      0 -> 0.0
      objects -> calculate_utilization_ratio(objects, dets_storage)
    end
  end

  defp calculate_utilization_ratio(objects, dets_storage) do
    file_size = :dets.info(dets_storage, :file_size) || 1
    min(1.0, objects / max(1, file_size / 1000))
  end
end
