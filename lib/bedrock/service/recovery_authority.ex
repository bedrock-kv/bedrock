defmodule Bedrock.Service.RecoveryAuthority do
  @moduledoc """
  Durable authority granted to one recovery attempt.

  Generations order attempts. The recovery id distinguishes owners within a
  generation; caller processes are deliberately absent from the identity.
  """

  @enforce_keys [:generation, :recovery_id]
  defstruct [:generation, :recovery_id]

  @type t :: %__MODULE__{generation: pos_integer(), recovery_id: binary()}
  @type input :: t() | %{generation: pos_integer(), recovery_id: binary()}

  @spec new(term()) :: {:ok, t()} | {:error, :invalid_recovery_authority}
  def new(%__MODULE__{} = authority), do: validate(authority)

  def new(%{generation: generation, recovery_id: recovery_id})
      when is_integer(generation) and generation > 0 and generation <= 0xFFFFFFFFFFFFFFFF and is_binary(recovery_id) and
             byte_size(recovery_id) > 0, do: {:ok, %__MODULE__{generation: generation, recovery_id: recovery_id}}

  def new(_), do: {:error, :invalid_recovery_authority}

  @spec new!(input()) :: t()
  def new!(input) do
    case new(input) do
      {:ok, authority} -> authority
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  @spec compare(input(), input()) :: :older | :same | :equal_generation_foreign | :newer
  def compare(left, right) do
    left = new!(left)
    right = new!(right)

    cond do
      left.generation < right.generation -> :older
      left.generation > right.generation -> :newer
      left.recovery_id == right.recovery_id -> :same
      true -> :equal_generation_foreign
    end
  end

  @spec external(input()) :: %{generation: pos_integer(), recovery_id: binary()}
  def external(authority), do: authority |> new!() |> Map.from_struct()

  defp validate(%__MODULE__{generation: generation, recovery_id: recovery_id} = authority) do
    if valid_generation?(generation) and is_binary(recovery_id) and byte_size(recovery_id) > 0 do
      {:ok, authority}
    else
      {:error, :invalid_recovery_authority}
    end
  end

  defp validate(_), do: {:error, :invalid_recovery_authority}

  defp valid_generation?(generation) do
    is_integer(generation) and generation > 0 and generation <= 0xFFFFFFFFFFFFFFFF
  end
end
