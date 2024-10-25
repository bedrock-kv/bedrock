defmodule Bedrock.DataPlane.Log.Limestone.Subscriptions do
  @type t :: :ets.table()
  @type subscription :: {
          id :: String.t(),
          last_version :: Bedrock.version(),
          last_durable_version :: Bedrock.version(),
          last_seen_at :: integer()
        }

  def new, do: :ets.new(:subscribers, [:ordered_set])

  @spec all(t()) :: {:ok, [subscription()]}
  def all(t), do: {:ok, :ets.tab2list(t)}

  @spec lookup(t(), String.t()) :: {:ok, subscription()} | {:error, :not_found}
  def lookup(t, id) do
    :ets.lookup(t, id)
    |> case do
      [] -> {:error, :not_found}
      [sub] -> {:ok, sub}
    end
  end

  @spec update(t(), String.t(), Bedrock.version(), Bedrock.version()) :: :ok
  def update(t, id, last_version, last_durable_version) do
    :ets.insert(
      t,
      {id, last_version, last_durable_version, DateTime.utc_now() |> DateTime.to_unix()}
    )

    :ok
  end

  @spec minimum_durable_version(t(), pos_integer()) :: :ets.match_spec()
  def minimum_durable_version(t, max_age_in_s) do
    :ets.select(t, match_last_seen_for_live_subscribers(max_age_in_s))
    |> case do
      [] -> nil
      last_durable_versions -> last_durable_versions |> Enum.min()
    end
  end

  @spec match_last_seen_for_live_subscribers(pos_integer()) :: :ets.match_spec()
  defp match_last_seen_for_live_subscribers(max_age_in_s) do
    threshold_time = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.-(max_age_in_s)

    [{{:_, :_, :"$3", :"$4"}, [{:>=, :"$4", threshold_time}], [:"$3"]}]
  end
end
