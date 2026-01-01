defmodule Bedrock.ControlPlane.Config.Persistence do
  @moduledoc """
  Handles encoding/decoding of cluster configuration for persistent storage.

  Converts between runtime PIDs and serializable {otp_name, node} tuples to enable
  cluster configuration persistence across restarts. This module provides a clean
  separation between runtime efficiency (using PIDs) and storage requirements
  (using serializable references).
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type otp_reference :: {atom(), node()}
  @type encoded_config :: map()
  @type service_status :: :down | {:up, pid()} | {:up, otp_reference()}
  @type encoded_service_status :: :down | {:up, otp_reference()}

  @doc """
  Encodes a cluster configuration for persistent storage.

  Converts all PIDs to {otp_name, node} tuples that can be safely serialized
  with BERT encoding. Removes ephemeral state that shouldn't be persisted.

  ## Parameters
  - `config`: The runtime cluster configuration
  - `cluster`: The cluster module for OTP name resolution

  ## Returns
  - Sanitized configuration suitable for BERT encoding
  """
  @spec encode_for_storage(Config.t(), module()) :: encoded_config()
  def encode_for_storage(config, cluster) do
    config
    |> remove_ephemeral_state()
    |> encode_transaction_system_layout(cluster)
  end

  @doc """
  Decodes a cluster configuration from persistent storage.

  Converts {otp_name, node} tuples back to PIDs for runtime use.

  ## Parameters
  - `encoded_config`: The configuration read from storage
  - `cluster`: The cluster module for OTP name resolution

  ## Returns
  - Runtime configuration with PIDs restored
  """
  @spec decode_from_storage(encoded_config(), module()) :: Config.t()
  def decode_from_storage(encoded_config, cluster) do
    decode_transaction_system_layout(encoded_config, cluster)
  end

  @doc """
  Encodes a transaction system layout for persistent storage.

  Converts all PIDs to {otp_name, node} tuples that can be safely serialized
  with BERT encoding.

  ## Parameters
  - `layout`: The runtime transaction system layout
  - `cluster`: The cluster module for OTP name resolution

  ## Returns
  - Sanitized layout suitable for BERT encoding
  """
  @spec encode_transaction_system_layout_for_storage(TransactionSystemLayout.t(), module()) ::
          map()
  def encode_transaction_system_layout_for_storage(layout, cluster) do
    layout
    |> encode_single_reference(:director, cluster)
    |> encode_single_reference(:sequencer, cluster)
    |> encode_single_reference(:rate_keeper, cluster)
    |> encode_proxy_list(cluster)
    |> encode_service_map(cluster)
  end

  @doc """
  Decodes a transaction system layout from persistent storage.

  Converts {otp_name, node} tuples back to PIDs for runtime use.

  ## Parameters
  - `encoded_layout`: The layout read from storage
  - `cluster`: The cluster module for OTP name resolution

  ## Returns
  - Runtime layout with PIDs restored
  """
  @spec decode_transaction_system_layout_from_storage(map(), module()) ::
          TransactionSystemLayout.t()
  def decode_transaction_system_layout_from_storage(encoded_layout, cluster) do
    encoded_layout
    |> decode_single_reference(:director, cluster)
    |> decode_single_reference(:sequencer, cluster)
    |> decode_single_reference(:rate_keeper, cluster)
    |> decode_proxy_list(cluster)
    |> decode_service_map(cluster)
  end

  # Remove state that shouldn't be persisted
  @spec remove_ephemeral_state(Config.t()) :: Config.t()
  defp remove_ephemeral_state(config) do
    Map.delete(config, :recovery_attempt)
    # Recovery state is ephemeral
  end

  # Encode transaction_system_layout field within config
  @spec encode_transaction_system_layout(Config.t(), module()) :: Config.t()
  defp encode_transaction_system_layout(config, cluster) do
    case config[:transaction_system_layout] do
      nil ->
        config

      layout ->
        encoded_layout = encode_transaction_system_layout_for_storage(layout, cluster)
        Map.put(config, :transaction_system_layout, encoded_layout)
    end
  end

  # Decode transaction_system_layout field within config
  @spec decode_transaction_system_layout(Config.t(), module()) :: Config.t()
  defp decode_transaction_system_layout(config, cluster) do
    case config[:transaction_system_layout] do
      nil ->
        config

      encoded_layout ->
        decoded_layout = decode_transaction_system_layout_from_storage(encoded_layout, cluster)
        Map.put(config, :transaction_system_layout, decoded_layout)
    end
  end

  # Generic single reference encoding (director, sequencer, rate_keeper)
  @spec encode_single_reference(map(), atom(), module()) :: map()
  defp encode_single_reference(layout, field, cluster) do
    case layout[field] do
      pid when is_pid(pid) ->
        Map.put(layout, field, pid_to_otp_reference(pid, cluster, field))

      nil ->
        layout

      :unavailable ->
        Map.put(layout, field, :unavailable)

      :unset ->
        layout
    end
  end

  # Generic single reference decoding
  @spec decode_single_reference(map(), atom(), module()) :: map()
  defp decode_single_reference(layout, field, _cluster) do
    case layout[field] do
      {otp_name, node} when is_atom(otp_name) and is_atom(node) ->
        Map.put(layout, field, otp_reference_to_pid({otp_name, node}))

      nil ->
        layout

      :unavailable ->
        layout

      :unset ->
        layout
    end
  end

  # Proxy list encoding
  @spec encode_proxy_list(map(), module()) :: map()
  defp encode_proxy_list(layout, cluster) do
    proxies = layout[:proxies] || []

    encoded_proxies =
      proxies
      |> Enum.with_index()
      |> Enum.map(fn {pid, index} ->
        pid_to_otp_reference(pid, cluster, "commit_proxy_#{index + 1}")
      end)

    Map.put(layout, :proxies, encoded_proxies)
  end

  # Proxy list decoding
  @spec decode_proxy_list(map(), module()) :: map()
  defp decode_proxy_list(layout, _cluster) do
    proxies = layout[:proxies] || []

    decoded_proxies =
      Enum.map(proxies, fn
        {otp_name, node} when is_atom(otp_name) and is_atom(node) ->
          otp_reference_to_pid({otp_name, node})

        other ->
          # Pass through non-tuple values
          other
      end)

    Map.put(layout, :proxies, decoded_proxies)
  end

  # Service map encoding
  @spec encode_service_map(map(), module()) :: map()
  defp encode_service_map(layout, cluster) do
    services = layout[:services] || %{}

    encoded_services =
      Map.new(services, fn {service_id, service_descriptor} ->
        encoded_descriptor = encode_service_status(service_descriptor, cluster, service_id)
        {service_id, encoded_descriptor}
      end)

    Map.put(layout, :services, encoded_services)
  end

  # Service map decoding
  @spec decode_service_map(map(), module()) :: map()
  defp decode_service_map(layout, _cluster) do
    services = layout[:services] || %{}

    decoded_services =
      Map.new(services, fn {service_id, service_descriptor} ->
        decoded_descriptor = decode_service_status(service_descriptor)
        {service_id, decoded_descriptor}
      end)

    Map.put(layout, :services, decoded_services)
  end

  # Service descriptor status encoding
  @spec encode_service_status(map(), module(), String.t()) :: map()
  defp encode_service_status(descriptor, cluster, service_id) do
    case descriptor[:status] do
      {:up, pid} when is_pid(pid) ->
        otp_reference = pid_to_otp_reference(pid, cluster, service_id)
        Map.put(descriptor, :status, {:up, otp_reference})

      other ->
        Map.put(descriptor, :status, other)
    end
  end

  # Service descriptor status decoding
  @spec decode_service_status(map()) :: map()
  defp decode_service_status(descriptor) do
    case descriptor[:status] do
      {:up, {otp_name, node}} when is_atom(otp_name) and is_atom(node) ->
        pid = otp_reference_to_pid({otp_name, node})
        Map.put(descriptor, :status, {:up, pid})

      other ->
        Map.put(descriptor, :status, other)
    end
  end

  # Helper to convert PID to {otp_name, node} tuple
  @spec pid_to_otp_reference(pid(), module(), String.t() | atom()) :: otp_reference()
  defp pid_to_otp_reference(pid, cluster, component) when is_pid(pid) do
    node = node(pid)
    otp_name = cluster.otp_name(component)
    {otp_name, node}
  end

  # Helper to convert {otp_name, node} tuple to PID
  @spec otp_reference_to_pid(otp_reference()) :: pid() | nil
  defp otp_reference_to_pid({otp_name, node}) when is_atom(otp_name) and is_atom(node) do
    if node == node() do
      # Local process - use Process.whereis directly
      Process.whereis(otp_name)
    else
      # Remote process - use RPC
      case :rpc.call(node, Process, :whereis, [otp_name]) do
        pid when is_pid(pid) -> pid
        nil -> nil
        {:badrpc, _reason} -> nil
      end
    end
  end
end
