defmodule Bedrock.ObjectStorage.S3 do
  @moduledoc """
  S3-compatible ObjectStorage backend.

  This backend supports AWS S3 and S3-compatible APIs (for example MinIO)
  through ExAws.
  """

  @behaviour Bedrock.ObjectStorage

  @impl true
  def put(config, key, data, _opts \\ []) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.put_object(key, data)
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(config, key) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.get_object(key)
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, %{body: body}} -> {:ok, body}
      {:error, {:http_error, 404, _details}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def delete(config, key) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.delete_object(key)
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, _response} -> :ok
      {:error, {:http_error, 404, _details}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def list(config, prefix, opts \\ []) do
    state = %{
      config: config,
      prefix: prefix,
      continuation_token: nil,
      buffer: [],
      emitted: 0,
      limit: Keyword.get(opts, :limit),
      exhausted?: false
    }

    Stream.resource(fn -> state end, &next_list_item/1, fn _ -> :ok end)
  end

  @impl true
  def put_if_not_exists(config, key, data, _opts \\ []) do
    case get(config, key) do
      {:error, :not_found} -> put(config, key, data)
      {:ok, _data} -> {:error, :already_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get_with_version(config, key) do
    case get(config, key) do
      {:ok, data} ->
        hash = :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
        {:ok, data, "sha256:#{hash}"}

      error ->
        error
    end
  end

  @impl true
  def put_if_version_matches(config, key, version_token, data, opts \\ []) do
    case get_with_version(config, key) do
      {:ok, _current_data, current_token} ->
        if current_token == version_token do
          put(config, key, data, opts)
        else
          {:error, :version_mismatch}
        end

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp next_list_item(%{limit: limit, emitted: emitted} = state) when is_integer(limit) and emitted >= limit do
    {:halt, state}
  end

  defp next_list_item(%{buffer: [key | rest]} = state) do
    {[key], %{state | buffer: rest, emitted: state.emitted + 1}}
  end

  defp next_list_item(%{exhausted?: true} = state) do
    {:halt, state}
  end

  defp next_list_item(state) do
    case fetch_page(state) do
      {:ok, keys, continuation_token, exhausted?} ->
        next_list_item(%{state | buffer: keys, continuation_token: continuation_token, exhausted?: exhausted?})

      {:error, _reason} ->
        {:halt, %{state | exhausted?: true}}
    end
  end

  defp fetch_page(state) do
    bucket = Keyword.fetch!(state.config, :bucket)

    page_size =
      case state.limit do
        nil -> 1_000
        limit -> min(limit - state.emitted, 1_000)
      end

    opts = maybe_put_continuation_token([prefix: state.prefix, max_keys: page_size], state.continuation_token)

    bucket
    |> ExAws.S3.list_objects_v2(opts)
    |> ExAws.request(request_config(state.config))
    |> case do
      {:ok, %{body: body}} ->
        keys = parse_keys(body)
        continuation_token = continuation_token(body)
        exhausted? = !truncated?(body)
        {:ok, keys, continuation_token, exhausted?}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp request_config(config), do: Keyword.get(config, :config, [])

  defp maybe_put_continuation_token(opts, nil), do: opts
  defp maybe_put_continuation_token(opts, token), do: Keyword.put(opts, :continuation_token, token)

  defp parse_keys(body) do
    body
    |> Map.get(:contents, Map.get(body, "Contents", []))
    |> Enum.map(fn content ->
      Map.get(content, :key, Map.get(content, "Key"))
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp continuation_token(body) do
    Map.get(body, :next_continuation_token, Map.get(body, "NextContinuationToken"))
  end

  defp truncated?(body) do
    case Map.get(body, :is_truncated, Map.get(body, "IsTruncated", false)) do
      true -> true
      "true" -> true
      _ -> false
    end
  end
end
