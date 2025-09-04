defmodule Bedrock.KeySelectorTestHelpers do
  @moduledoc """
  Test helpers for KeySelector cross-shard testing with dependency injection.

  Provides utilities to mock LayoutIndex and Storage protocol calls to test
  cross-shard KeySelector resolution scenarios without requiring a full cluster.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.KeySelector

  @doc """
  Creates a mock LayoutIndex that can be used for testing cross-shard scenarios.

  ## Parameters

  - `shard_configs`: List of `{shard_id, start_key, end_key}` tuples defining shard boundaries

  ## Examples

      iex> layout_index = create_test_layout_index([
      ...>   {"shard1", "a", "m"},
      ...>   {"shard2", "n", "z"}
      ...> ])
      iex> LayoutIndex.shard_for_key(layout_index, "j")
      "shard1"
  """
  @spec create_test_layout_index([{binary(), binary(), binary()}]) :: LayoutIndex.t()
  def create_test_layout_index(shard_configs) do
    %LayoutIndex{
      shards: create_shard_map(shard_configs),
      version: 1
    }
  end

  @doc """
  Sets up mock storage server responses for testing.

  Uses the process dictionary to store mock responses that can be retrieved
  during test execution.

  ## Parameters

  - `response_map`: Map of `shard_id => response_function`

  ## Examples

      setup_storage_mocks(%{
        "shard1" => fn key_selector, version ->
          {:ok, {"resolved_key", "resolved_value"}}
        end,
        "shard2" => fn key_selector, version ->
          {:partial, 5}  # 5 keys available, need continuation
        end
      })
  """
  @spec setup_storage_mocks(%{binary() => function()}) :: :ok
  def setup_storage_mocks(response_map) do
    Enum.each(response_map, fn {shard_id, response_fn} ->
      Process.put({:test_storage_mock, shard_id}, response_fn)
    end)

    :ok
  end

  @doc """
  Sets up mock storage server responses for range queries.
  """
  @spec setup_range_storage_mocks(%{binary() => function()}) :: :ok
  def setup_range_storage_mocks(response_map) do
    Enum.each(response_map, fn {shard_id, response_fn} ->
      Process.put({:test_range_storage_mock, shard_id}, response_fn)
    end)

    :ok
  end

  @doc """
  Retrieves a mock storage response for a given shard.

  This function would be called by the KeySelectorResolution module
  during testing to get mock responses instead of making real network calls.
  """
  @spec get_storage_mock_response(binary(), KeySelector.t(), integer()) ::
          {:ok, {binary(), binary()}} | {:partial, integer()} | {:error, atom()}
  def get_storage_mock_response(shard_id, key_selector, version) do
    case Process.get({:test_storage_mock, shard_id}) do
      nil -> {:error, :no_mock_configured}
      response_fn -> response_fn.(key_selector, version)
    end
  end

  @doc """
  Retrieves a mock range storage response for a given shard.
  """
  @spec get_range_storage_mock_response(binary(), KeySelector.t(), KeySelector.t(), integer(), keyword()) ::
          {:ok, [{binary(), binary()}]} | {:error, atom()}
  def get_range_storage_mock_response(shard_id, start_selector, end_selector, version, opts) do
    case Process.get({:test_range_storage_mock, shard_id}) do
      nil -> {:error, :no_mock_configured}
      response_fn -> response_fn.(start_selector, end_selector, version, opts)
    end
  end

  @doc """
  Creates KeySelector test scenarios with predictable cross-shard behavior.

  Returns a list of test cases with expected outcomes.
  """
  @spec create_test_scenarios() :: [
          %{
            name: binary(),
            key_selector: KeySelector.t(),
            expected_shard_crossings: integer(),
            expected_result: {:ok, {binary(), binary()}} | {:error, atom()}
          }
        ]
  def create_test_scenarios do
    [
      %{
        name: "single shard resolution",
        key_selector: KeySelector.first_greater_or_equal("j"),
        expected_shard_crossings: 0,
        expected_result: {:ok, {"j", "value_j"}}
      },
      %{
        name: "cross shard with small offset",
        key_selector: "k" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5),
        expected_shard_crossings: 1,
        expected_result: {:ok, {"p", "value_p"}}
      },
      %{
        name: "cross shard with large offset",
        key_selector: "a" |> KeySelector.first_greater_or_equal() |> KeySelector.add(50),
        expected_shard_crossings: 2,
        expected_result: {:ok, {"z", "value_z"}}
      },
      %{
        name: "backward cross shard",
        key_selector: "p" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-10),
        expected_shard_crossings: 1,
        expected_result: {:ok, {"f", "value_f"}}
      },
      %{
        name: "offset too large - clamped",
        key_selector: "a" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000),
        expected_shard_crossings: 5,
        expected_result: {:error, :clamped}
      }
    ]
  end

  @doc """
  Creates range KeySelector test scenarios.
  """
  @spec create_range_test_scenarios() :: [
          %{
            name: binary(),
            start_selector: KeySelector.t(),
            end_selector: KeySelector.t(),
            expected_shards: [binary()],
            expected_result: {:ok, [{binary(), binary()}]} | {:error, atom()}
          }
        ]
  def create_range_test_scenarios do
    [
      %{
        name: "single shard range",
        start_selector: KeySelector.first_greater_or_equal("d"),
        end_selector: KeySelector.first_greater_than("j"),
        expected_shards: ["shard1"],
        expected_result: {:ok, [{"e", "value_e"}, {"f", "value_f"}]}
      },
      %{
        name: "cross shard range",
        start_selector: KeySelector.first_greater_or_equal("k"),
        end_selector: KeySelector.first_greater_than("r"),
        expected_shards: ["shard1", "shard2"],
        expected_result: {:ok, [{"l", "value_l"}, {"m", "value_m"}, {"n", "value_n"}]}
      },
      %{
        name: "multi shard range - clamped",
        start_selector: KeySelector.first_greater_or_equal("b"),
        end_selector: KeySelector.first_greater_than("y"),
        expected_shards: ["shard1", "shard2", "shard3"],
        expected_result: {:error, :clamped}
      }
    ]
  end

  @doc """
  Asserts that a KeySelector resolution follows expected cross-shard behavior.
  """
  @spec assert_cross_shard_behavior(
          KeySelector.t(),
          {:ok, {binary(), binary()}} | {:error, atom()},
          integer()
        ) :: :ok
  def assert_cross_shard_behavior(key_selector, expected_result, expected_crossings) do
    # Get the number of shard boundary crossings that occurred during resolution
    crossings = Process.get(:shard_crossings, 0)

    # Verify the crossing count matches expectations
    if crossings != expected_crossings do
      raise "Expected #{expected_crossings} shard crossings but got #{crossings} for KeySelector #{KeySelector.to_string(key_selector)}"
    end

    :ok
  end

  @doc """
  Cleans up test state between test runs.
  """
  @spec cleanup_test_state() :: :ok
  def cleanup_test_state do
    # Clear all mock configurations from process dictionary
    Process.get()
    |> Enum.filter(fn {key, _value} ->
      match?({:test_storage_mock, _}, key) or
        match?({:test_range_storage_mock, _}, key)
    end)
    |> Enum.each(fn {key, _value} -> Process.delete(key) end)

    Process.delete(:shard_crossings)
    :ok
  end

  # Private helper functions

  defp create_shard_map(shard_configs) do
    Map.new(shard_configs, fn {shard_id, start_key, end_key} ->
      {shard_id,
       %{
         id: shard_id,
         start_key: start_key,
         end_key: end_key,
         storage_server: "mock_server_#{shard_id}"
       }}
    end)
  end
end
