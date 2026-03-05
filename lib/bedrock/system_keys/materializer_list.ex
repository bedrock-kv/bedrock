defmodule Bedrock.SystemKeys.MaterializerList do
  @moduledoc """
  FlatBuffer schema for materializers serving a key range.

  Key pattern: `\\xff/system/materializer_keys/{end_key}`

  ## Example

      # Encode
      materializers = [{:mat1, :node1}, {:mat2, :node2}]
      binary = MaterializerList.from_tuples(materializers)

      # Decode
      materializers = MaterializerList.to_tuples(binary)
  """

  use Flatbuffer, file: "priv/schemas/materializer_list.fbs"

  @type t :: [{atom(), atom()}]

  @doc "Converts a binary MaterializerList to list of Elixir tuples"
  @spec to_tuples(binary()) :: t() | {:error, term()}
  def to_tuples(binary) do
    case read(binary) do
      {:ok, %{materializers: mats}} ->
        Enum.map(mats, fn %{otp_name: name, node: node} ->
          {String.to_atom(name), String.to_atom(node)}
        end)

      error ->
        error
    end
  end

  @doc "Converts a list of Elixir tuples to binary MaterializerList"
  @spec from_tuples(t()) :: binary()
  def from_tuples(materializers) do
    to_binary(%{
      materializers:
        Enum.map(materializers, fn {otp_name, node} ->
          %{otp_name: Atom.to_string(otp_name), node: Atom.to_string(node)}
        end)
    })
  end
end
