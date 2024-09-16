defmodule Bedrock.ControlPlane.DataDistributor.LogInfoTable do
  alias Bedrock.ControlPlane.DataDistributor.LogInfo

  @type t :: :ets.table()

  @spec new() :: t()
  def new, do: :ets.new(:log_table, [:protected, :duplicate_bag, read_concurrency: true])

  @doc """
  Add a log_info to the table. The log_info is expected to have an id that does
  not conflict with any other log_info in the table. It's okay if the log_info's
  tag is the same as another log_info's tag.
  """
  @spec add_log_info(t(), LogInfo.t()) :: :ok | {:error, :already_exists}
  def add_log_info(t, log_info) do
    :ets.insert_new(t, log_info |> to_record(:id))
    |> case do
      true ->
        true = :ets.insert(t, log_info |> to_record(:tag))
        :ok

      false ->
        {:error, :already_exists}
    end
  end

  @doc """
  Find all log_infos for the given tag, or return :not_found.
  """
  @spec log_infos_for_tag(t(), LogInfo.tag()) :: {:ok, [LogInfo.t()]} | {:error, :not_found}
  def log_infos_for_tag(t, tag) do
    :ets.lookup(t, {:tag, tag})
    |> case do
      [] -> {:error, :not_found}
      records -> {:ok, records |> Enum.map(&to_log_info/1)}
    end
  end

  @doc """
  Find the log_info for the given id, or return :not_found.
  """
  @spec log_info_for_id(t(), id :: LogInfo.id()) :: {:ok, LogInfo.t()} | {:error, :not_found}
  def log_info_for_id(t, id) do
    :ets.lookup(t, {:id, id})
    |> case do
      [] -> {:error, :not_found}
      [record] -> {:ok, record |> to_log_info()}
    end
  end

  defp to_log_info({_key, id, tag, endpoint} = _record),
    do: LogInfo.new(id, tag, endpoint)

  defp to_record(log_info, :tag),
    do: {{:tag, log_info.tag}, log_info.id, log_info.tag, log_info.endpoint}

  defp to_record(log_info, :id),
    do: {{:id, log_info.id}, log_info.id, log_info.tag, log_info.endpoint}
end
