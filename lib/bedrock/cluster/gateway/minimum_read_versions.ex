defmodule Bedrock.Cluster.Gateway.MinimumReadVersions do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Internal.Time

  @type deadline_by_version :: %{Bedrock.version() => Bedrock.timestamp_in_ms()}

  @spec recalculate_minimum_read_version(State.t()) :: State.t()
  def recalculate_minimum_read_version(t) do
    {minimum_read_version, deadline_by_version} =
      find_minimum_active_version(
        t.deadline_by_version,
        Time.monotonic_now_in_ms()
      )

    t
    |> Map.put(:deadline_by_version, deadline_by_version)
    |> Map.put(:minimum_read_version, minimum_read_version)
  end

  @doc """
  Find the minimum active version with a deadline greater than the current time.

  Iterates over the `deadline_by_version` map to determine the minimum version with
  a deadline in the future. Returns a tuple containing this minimum version and an
  updated map of versions which still have valid deadlines. It returns `nil` if
  no versions have a deadline in the future and an empty map if all deadlines
  have expired.
  """
  @spec find_minimum_active_version(deadline_by_version(), at :: Bedrock.timestamp_in_ms()) ::
          {minimum_version :: Bedrock.version() | nil, deadline_by_version()}
  def find_minimum_active_version(deadline_by_version, at) do
    Enum.reduce(deadline_by_version, {nil, %{}}, fn
      {read_version, deadline}, {minimum_version, deadline_by_version} when deadline > at ->
        {min(read_version, minimum_version), Map.put(deadline_by_version, read_version, deadline)}

      _, acc ->
        acc
    end)
  end
end
