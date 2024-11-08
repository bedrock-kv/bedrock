defmodule Bedrock.DataPlane.Log.Shale.Pulling do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  @spec pull(
          t :: State.t(),
          from_version :: Bedrock.version() | :start,
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            recovery: boolean()
          ]
        ) ::
          {:ok, State.t(), [Transaction.t()]}
          | {:waiting_for, Bedrock.version()}
          | {:error, :not_ready}
          | {:error, :not_locked}
          | {:error, :invalid_from_version}
          | {:error, :invalid_last_version}
          | {:error, :version_too_old}
          | {:error, :version_not_found}
  def pull(t, from_version, opts) do
    with :ok <- check_for_locked_outside_of_recovery(opts[:recovery] || false, t),
         :ok <- check_from_version(from_version, t),
         {:ok, last_version} <- check_last_version(opts[:last_version], from_version),
         limit <- determine_pull_limit(opts[:limit], t) do
      :ets.select(t.log, match_spec_for_version_range(from_version, last_version), limit)
      |> case do
        {[], _} ->
          {:error, :invalid_from_version}

        :"$end_of_table" ->
          {:ok, t, []}

        {[{^from_version, _}], _} ->
          {:ok, t, []}

        {[{^from_version, _} | transactions], _} ->
          {:ok, t, transactions}

        {transactions, _} when from_version == :start ->
          {:ok, t, transactions}

        _ ->
          {:error, :version_not_found}
      end
    end
  end

  def match_spec_for_version_range(:start, last_version),
    do: match_spec_for_version_lte(last_version)

  def match_spec_for_version_range(from_version, nil),
    do: match_spec_for_version_gte(from_version)

  def match_spec_for_version_range(from_version, last_version) do
    [
      {
        {:"$1", :_},
        [{:>=, :"$1", from_version}, {:"=<", :"$1", last_version}],
        [:"$_"]
      }
    ]
  end

  def match_spec_for_version_gte(version) do
    [
      {
        {:"$1", :_},
        [{:>=, :"$1", version}],
        [:"$_"]
      }
    ]
  end

  def match_spec_for_version_lte(version) do
    [
      {
        {:"$1", :_},
        [{:"=<", :"$1", version}],
        [:"$_"]
      }
    ]
  end

  @spec check_for_locked_outside_of_recovery(boolean(), State.t()) ::
          :ok | {:error, :not_locked} | {:error, :not_ready}
  def check_for_locked_outside_of_recovery(in_recovery, t)
  def check_for_locked_outside_of_recovery(true, %{mode: :locked}), do: :ok
  def check_for_locked_outside_of_recovery(true, _), do: {:error, :not_locked}
  def check_for_locked_outside_of_recovery(false, %{mode: :locked}), do: {:error, :not_ready}
  def check_for_locked_outside_of_recovery(_, _), do: :ok

  @spec check_from_version(
          from_version :: Bedrock.version() | :start,
          t :: State.t()
        ) :: :ok | {:waiting_for, Bedrock.version()} | {:error, :version_too_old}
  def check_from_version(:start, _t), do: :ok

  def check_from_version(from_version, t) when t.last_version <= from_version,
    do: {:waiting_for, from_version}

  def check_from_version(_, t) when t.oldest_version == :start,
    do: :ok

  def check_from_version(from_version, t) when t.oldest_version > from_version,
    do: {:error, :version_too_old}

  def check_from_version(_, _), do: :ok

  @spec check_last_version(
          last_version :: Bedrock.version() | nil,
          from_version :: Bedrock.version() | :start
        ) :: {:ok, Bedrock.version()} | {:error, :invalid_last_version}
  def check_last_version(nil, _), do: {:ok, nil}

  def check_last_version(last_version, from_version)
      when last_version >= from_version or from_version == :start,
      do: {:ok, last_version}

  def check_last_version(_, _), do: {:error, :invalid_last_version}

  def determine_pull_limit(nil, t), do: t.params.default_pull_limit
  def determine_pull_limit(limit, t), do: min(limit, t.params.max_pull_limit)
end
