defmodule Bedrock.SystemKeys do
  @moduledoc """
  The `\\xFF/system` keys Bedrock writes and reads.

  Every key defined here has a named purpose: `shard_keys/<end_key>` feeds
  each commit proxy's routing view (through resolver metadata windows) and
  the next recovery's materializer bootstrap; `materializers/<tag>` refs
  feed the client-facing routing projection served by commit proxies
  (FDB's `serverList/` analogue - interfaces ride the keyspace) and answer
  worker rejoin validation. `layout/logs/<log_id>` keys have no code
  reader by design: log wiring is epoch-constant and rides the recovery
  unlock seed (bedrock-q67.41) - the family stays durable for other
  consumers and cluster-introspection tools, a queryable statement of
  which logs the current epoch runs. Materializer refs are runtime hints
  for clients, never recovery input: bootstrap rebuilds assignment from
  `shard_keys/` plus live foreman discovery. A system key without a
  reader is inventory, not communication - unread MACHINERY is deleted,
  while durable observability keys stay by decision, named as such;
  families return here when their readers do (config authority with
  bedrock-q67.25).
  """

  @system_prefix "\xff/system"

  @doc "Shard boundary entry: `shard_keys/<end_key>` -> `{tag, start_key}` (ceiling search)"
  @spec shard_key(Bedrock.key()) :: Bedrock.key()
  def shard_key(end_key), do: "#{@system_prefix}/shard_keys/#{end_key}"

  @doc "Prefix covering every shard boundary entry"
  @spec shard_keys_prefix() :: Bedrock.key()
  def shard_keys_prefix, do: "#{@system_prefix}/shard_keys/"

  @doc "Log layout entry: `layout/logs/<log_id>` -> tag list"
  @spec layout_log(Bedrock.range_tag() | String.t()) :: Bedrock.key()
  def layout_log(log_id), do: "#{@system_prefix}/layout/logs/#{log_id}"

  @doc "Prefix covering every log layout entry"
  @spec layout_logs_prefix() :: Bedrock.key()
  def layout_logs_prefix, do: "#{@system_prefix}/layout/logs/"

  @doc "Materializer ref entry: `materializers/<tag>` -> `{worker_id, node}` strings"
  @spec materializer_key(Bedrock.range_tag()) :: Bedrock.key()
  def materializer_key(tag) when is_integer(tag), do: "#{@system_prefix}/materializers/#{tag}"

  @doc "Prefix covering every materializer ref entry"
  @spec materializers_prefix() :: Bedrock.key()
  def materializers_prefix, do: "#{@system_prefix}/materializers/"

  @doc """
  Parses a system key into its family. Unknown system keys parse as
  `:unknown` (forward compatibility); non-system keys as `:error`.
  """
  @spec parse_key(Bedrock.key()) ::
          {:layout_log, String.t()}
          | {:shard_key, Bedrock.key()}
          | {:materializer_key, Bedrock.range_tag()}
          | :unknown
          | :error
  def parse_key(<<@system_prefix, "/layout/logs/", rest::binary>>), do: {:layout_log, rest}
  def parse_key(<<@system_prefix, "/shard_keys/", rest::binary>>), do: {:shard_key, rest}

  def parse_key(<<@system_prefix, "/materializers/", rest::binary>>) do
    case Integer.parse(rest) do
      {tag, ""} -> {:materializer_key, tag}
      _ -> :unknown
    end
  end

  def parse_key(<<@system_prefix, _rest::binary>>), do: :unknown
  def parse_key(_key), do: :error
end
