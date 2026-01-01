defmodule Bedrock.KeySelector do
  @moduledoc """
  A KeySelector describes a key in the database that can be resolved to an actual key at runtime.

  KeySelectors leverage the lexicographically ordered nature of keys to find keys based on their
  positional relationships to other keys, without needing to know exact key values.

  ## Examples

      iex> KeySelector.first_greater_or_equal("user:")
      %KeySelector{key: "user:", or_equal: true, offset: 0}
      
      iex> KeySelector.first_greater_than("data") |> KeySelector.add(5)
      %KeySelector{key: "data", or_equal: false, offset: 6}
  """

  @type t :: %__MODULE__{
          key: binary(),
          or_equal: boolean(),
          offset: integer()
        }

  defstruct key: <<>>, or_equal: true, offset: 0

  @doc """
  Creates a KeySelector that resolves to the lexicographically smallest key
  greater than or equal to the given key.
  """
  @spec first_greater_or_equal(binary()) :: t()
  def first_greater_or_equal(key) when is_binary(key), do: %__MODULE__{key: key, or_equal: true, offset: 0}

  @doc """
  Creates a KeySelector that resolves to the lexicographically smallest key
  strictly greater than the given key.
  """
  @spec first_greater_than(binary()) :: t()
  def first_greater_than(key) when is_binary(key), do: %__MODULE__{key: key, or_equal: true, offset: 1}

  @doc """
  Creates a KeySelector that resolves to the lexicographically largest key
  less than or equal to the given key.
  """
  @spec last_less_or_equal(binary()) :: t()
  def last_less_or_equal(key) when is_binary(key), do: %__MODULE__{key: key, or_equal: true, offset: -1}

  @doc """
  Creates a KeySelector that resolves to the lexicographically largest key
  strictly less than the given key.
  """
  @spec last_less_than(binary()) :: t()
  def last_less_than(key) when is_binary(key), do: %__MODULE__{key: key, or_equal: false, offset: 0}

  @doc """
  Adds an offset to a KeySelector, moving the resolution point forward (positive)
  or backward (negative) by the specified number of keys.

  ## Examples

      iex> KeySelector.first_greater_or_equal("a") |> KeySelector.add(3)
      %KeySelector{key: "a", or_equal: true, offset: 3}
      
      iex> KeySelector.first_greater_than("z") |> KeySelector.add(-2)
      %KeySelector{key: "z", or_equal: false, offset: -1}
  """
  @spec add(t(), integer()) :: t()
  def add(%__MODULE__{} = selector, offset) when is_integer(offset), do: %{selector | offset: selector.offset + offset}

  @doc """
  Subtracts an offset from a KeySelector, moving the resolution point backward (positive)
  or forward (negative) by the specified number of keys.

  ## Examples

      iex> KeySelector.first_greater_or_equal("m") |> KeySelector.subtract(2)
      %KeySelector{key: "m", or_equal: true, offset: -2}
  """
  @spec subtract(t(), integer()) :: t()
  def subtract(%__MODULE__{} = selector, offset) when is_integer(offset), do: add(selector, -offset)

  @doc """
  Returns true if the KeySelector has a zero offset, meaning it resolves
  to a key based only on the base key and or_equal flag.
  """
  @spec zero_offset?(t()) :: boolean()
  def zero_offset?(%__MODULE__{offset: 0}), do: true
  def zero_offset?(%__MODULE__{}), do: false

  @doc """
  Returns true if the KeySelector has a positive offset, meaning it will
  resolve to a key that comes after the reference position.
  """
  @spec positive_offset?(t()) :: boolean()
  def positive_offset?(%__MODULE__{offset: offset}) when offset > 0, do: true
  def positive_offset?(%__MODULE__{}), do: false

  @doc """
  Returns true if the KeySelector has a negative offset, meaning it will
  resolve to a key that comes before the reference position.
  """
  @spec negative_offset?(t()) :: boolean()
  def negative_offset?(%__MODULE__{offset: offset}) when offset < 0, do: true
  def negative_offset?(%__MODULE__{}), do: false

  @doc """
  Converts a KeySelector to a human-readable string representation.

  ## Examples

      iex> KeySelector.first_greater_or_equal("user:") |> KeySelector.to_string()
      "first_greater_or_equal(\"user:\")"
      
      iex> KeySelector.first_greater_than("data") |> KeySelector.add(3) |> KeySelector.to_string()
      "first_greater_than(\"data\") + 3"
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{key: key, or_equal: true, offset: 0}) do
    ~s{first_greater_or_equal(#{inspect(key)})}
  end

  def to_string(%__MODULE__{key: key, or_equal: true, offset: 1}) do
    ~s{first_greater_than(#{inspect(key)})}
  end

  def to_string(%__MODULE__{key: key, or_equal: true, offset: -1}) do
    ~s{last_less_or_equal(#{inspect(key)})}
  end

  def to_string(%__MODULE__{key: key, or_equal: false, offset: 0}) do
    ~s{last_less_than(#{inspect(key)})}
  end

  def to_string(%__MODULE__{key: key, or_equal: or_equal, offset: offset}) do
    # Always use first_greater_or_equal as canonical base and adjust offset
    canonical_offset = if or_equal, do: offset, else: offset - 1

    base_str = ~s{first_greater_or_equal(#{inspect(key)})}

    cond do
      canonical_offset > 0 -> "#{base_str} + #{canonical_offset}"
      canonical_offset < 0 -> "#{base_str} - #{abs(canonical_offset)}"
    end
  end
end

defimpl String.Chars, for: Bedrock.KeySelector do
  def to_string(key_selector) do
    Bedrock.KeySelector.to_string(key_selector)
  end
end
