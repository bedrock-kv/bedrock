defmodule Bedrock.DataPlane.Log.Shale.Pulling do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  @spec pull(
          t :: State.t(),
          from_version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            recovery: boolean()
          ]
        ) ::
          {:ok, [Transaction.t()]}
          | {:error, :not_ready}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
          | {:error, :version_not_found}
  def pull(t, from_version, opts) do
    cond do
      t.mode == :locked && !Keyword.get(opts, :recovery) ->
        {:error, :not_ready}

      t.last_version > from_version ->
        {:error, :version_too_new}

      from_version < t.oldest_version ->
        {:error, :version_too_old}

      true ->
        limit = Keyword.get(opts, :limit, 100)
        last_version = Keyword.get(opts, :last_version)

        :ets.select(t.log, match_spec_for_version_range(from_version, last_version), limit)
        |> case do
          {[], _} ->
            {:error, :version_not_found}

          :"$end_of_table" ->
            {:ok, []}

          {[{^from_version, _}], _} ->
            {:ok, []}

          {[{^from_version, _} | transactions], _} ->
            {:ok, transactions}
        end
    end
  end

  def match_spec_for_version_gte(min_version) do
    [{{:"$1", :_}, [{:>=, :"$1", min_version}], [:"$_"]}]
  end

  def match_spec_for_version_range(min_version, nil),
    do: match_spec_for_version_gte(min_version)

  def match_spec_for_version_range(min_version, max_version_exclusive) do
    [{{:"$1", :_}, [{:>=, :"$1", min_version}, {:<, :"$1", max_version_exclusive}], [:"$_"]}]
  end
end
