defmodule Bedrock.SystemKeys.OtpRef do
  @moduledoc """
  FlatBuffer schema for OtpRef - service identity.

  Used for all services (ephemeral and durable). Durable service IDs
  (like log_id) are encoded in the key path, not in this value.

  ## Example

      # Encode
      binary = OtpRef.to_binary(%{otp_name: "bedrock_sequencer", node: "node@host"})

      # Decode
      {:ok, %{otp_name: name, node: node}} = OtpRef.read(binary)

      # Convert to/from Elixir tuple format
      ref = OtpRef.to_tuple(binary)  # {:bedrock_sequencer, :"node@host"}
      binary = OtpRef.from_tuple({:bedrock_sequencer, :"node@host"})
  """

  use Flatbuffer, file: "priv/schemas/otp_ref.fbs"

  @type t :: {atom(), atom()}

  @doc "Converts a binary OtpRef to Elixir tuple format"
  @spec to_tuple(binary()) :: t() | {:error, term()}
  def to_tuple(binary) do
    case read(binary) do
      {:ok, %{otp_name: name, node: node}} ->
        {String.to_atom(name), String.to_atom(node)}

      error ->
        error
    end
  end

  @doc "Converts an Elixir tuple to binary OtpRef"
  @spec from_tuple(t()) :: binary()
  def from_tuple({otp_name, node}) do
    to_binary(%{otp_name: Atom.to_string(otp_name), node: Atom.to_string(node)})
  end
end
