defmodule Bedrock.ObjectStorage.S3 do
  @moduledoc """
  S3-compatible ObjectStorage backend.

  This backend supports AWS S3 and S3-compatible APIs (for example MinIO)
  through ExAws.
  """

  @behaviour Bedrock.ObjectStorage

  @impl true
  def put(config, key, data, opts \\ []) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.put_object(key, data, put_object_opts(opts))
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
  def put_if_not_exists(config, key, data, opts \\ []) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.put_object(key, data, put_object_opts(opts, if_none_match: "*"))
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, _response} ->
        :ok

      {:error, {:http_error, 412, _details}} ->
        {:error, :already_exists}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_with_version(config, key) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.get_object(key)
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, %{body: body} = response} ->
        with_etag(config, key, body, Map.get(response, :headers, []))

      {:error, {:http_error, 404, _details}} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp with_etag(config, key, body, headers) do
    case extract_etag(headers) do
      nil ->
        with {:ok, etag} <- head_etag(config, key) do
          {:ok, body, etag}
        end

      etag ->
        {:ok, body, etag}
    end
  end

  @impl true
  def put_if_version_matches(config, key, version_token, data, opts \\ []) do
    with {:ok, current_token} <- head_etag(config, key),
         true <- current_token == version_token or {:error, :version_mismatch} do
      bucket = Keyword.fetch!(config, :bucket)

      bucket
      |> ExAws.S3.put_object(key, data, put_object_opts(opts, if_match: version_token))
      |> ExAws.request(request_config(config))
      |> case do
        {:ok, _response} ->
          :ok

        {:error, {:http_error, 404, _details}} ->
          {:error, :not_found}

        {:error, {:http_error, 412, _details}} ->
          resolve_conditional_write_failure(config, key)

        {:error, reason} ->
          {:error, reason}
      end
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

  defp resolve_conditional_write_failure(config, key) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.head_object(key)
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, _response} ->
        {:error, :version_mismatch}

      {:error, {:http_error, 404, _details}} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp head_etag(config, key) do
    bucket = Keyword.fetch!(config, :bucket)

    bucket
    |> ExAws.S3.head_object(key)
    |> ExAws.request(request_config(config))
    |> case do
      {:ok, %{headers: headers}} ->
        case extract_etag(headers) do
          nil -> {:error, :version_mismatch}
          etag -> {:ok, etag}
        end

      {:ok, _response} ->
        {:error, :version_mismatch}

      {:error, {:http_error, 404, _details}} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp extract_etag(headers) when is_list(headers) do
    Enum.find_value(headers, fn
      {name, value} when is_binary(name) ->
        if String.downcase(name) == "etag", do: value

      {name, value} when is_atom(name) ->
        if String.downcase(to_string(name)) == "etag", do: value

      _ ->
        nil
    end)
  end

  defp extract_etag(headers) when is_map(headers) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(to_string(name)) == "etag", do: value
    end)
  end

  defp extract_etag(_headers), do: nil

  defp request_config(config), do: Keyword.get(config, :config, [])

  defp put_object_opts(opts, conditional_opts \\ []) do
    content_type_opt =
      case Keyword.get(opts, :content_type) do
        nil -> []
        content_type -> [content_type: content_type]
      end

    conditional_opts ++ content_type_opt
  end

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
