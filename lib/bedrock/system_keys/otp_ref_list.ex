defmodule Bedrock.SystemKeys.OtpRefList do
  @moduledoc """
  FlatBuffer schema for a list of OtpRefs.

  Used for proxies, coordinators, and other multi-service keys.

  ## Example

      # Encode
      refs = [{:proxy1, :node1}, {:proxy2, :node2}]
      binary = OtpRefList.from_tuples(refs)

      # Decode
      refs = OtpRefList.to_tuples(binary)
  """

  use Flatbuffer, file: "priv/schemas/otp_ref_list.fbs"

  @type t :: [{atom(), atom()}]

  @doc "Converts a binary OtpRefList to list of Elixir tuples"
  @spec to_tuples(binary()) :: t() | {:error, term()}
  def to_tuples(binary) do
    case read(binary) do
      {:ok, %{refs: refs}} ->
        Enum.map(refs, fn %{otp_name: name, node: node} ->
          {String.to_atom(name), String.to_atom(node)}
        end)

      error ->
        error
    end
  end

  @doc "Converts a list of Elixir tuples to binary OtpRefList"
  @spec from_tuples(t()) :: binary()
  def from_tuples(refs) do
    to_binary(%{
      refs:
        Enum.map(refs, fn {otp_name, node} ->
          %{otp_name: Atom.to_string(otp_name), node: Atom.to_string(node)}
        end)
    })
  end
end
