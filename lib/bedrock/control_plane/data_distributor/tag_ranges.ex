defmodule Bedrock.ControlPlane.DataDistributor.TagRanges do
  @moduledoc """
  `TagRanges` maps non-overlapping key ranges to integer tags and provides
  fast way to lookup the tag that owns a given key. The key ranges are
  represented by an ETS table that contains tuples of the form
  `{lower_bound, upper_bound, tag}`.
  """

  @type t :: :ets.table()
  @type tag :: non_neg_integer()
  @type key :: Bedrock.key()
  @type key_range :: Bedrock.key_range()

  @spec new() :: t()
  def new, do: :ets.new(:tag_list, [:ordered_set, read_concurrency: true])

  @doc """
  Find the tag that owns the given key. The key range for a tag is inclusive
  at the lower bound, and exclusive at the upper bound. This means that if a
  tag owns the range {"a", "b"}, then the key "a" is owned by the tag, but
  the key "b" is not, and that the key "aa" is also owned by the tag.
  """
  @spec tag_for_key(t(), key()) :: {:ok, tag()} | {:error, :not_found}
  def tag_for_key(t, key) when is_binary(key) do
    :ets.select(t, match_key_in_range(key), 1)
    |> case do
      {[tag], _continuation} ->
        {:ok, tag}

      :"$end_of_table" ->
        {:error, :not_found}
    end
  end

  @doc """
  Add a tag to the list of tags. The tag is expected to have a key range
  that does not overlap with any other tag in the list. It's okay if the
  tag's key range ends at the lower bound of another tag, or starts at the
  upper bound of another tag.
  """
  @spec add_tag(t(), tag(), key_range()) :: :ok | {:error, :key_range_overlaps}
  def add_tag(t, tag, {lower, upper}) when lower < upper do
    :ets.select(t, match_key_range_overlap(lower, upper), 1)
    |> case do
      {[_overlapping_tag], _continuation} ->
        {:error, :key_range_overlaps}

      :"$end_of_table" ->
        true = :ets.insert_new(t, {lower, upper, tag})
        :ok
    end
  end

  defp match_key_in_range(k),
    do: [{{:"$1", :"$2", :"$3"}, [{:and, {:"=<", :"$1", k}, {:>, :"$2", k}}], [:"$3"]}]

  defp match_key_range_overlap(l, u),
    do: [{{:"$1", :"$2", :"$3"}, [{:and, {:<, :"$1", u}, {:<, l, :"$2"}}], [:"$3"]}]
end
