defmodule Bedrock do
  @type key :: binary()
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key()}
  @type value :: binary()
  @type key_value :: {key(), value()}

  @type version :: non_neg_integer()
  @type version_vector :: {oldest :: version(), newest :: version()} | {:undefined, 0}

  @type transaction ::
          {read_version :: version(), reads :: [key() | key_range()],
           writes :: [key_value() | {:clear, key_range()}]}

  @type epoch :: non_neg_integer()
  @type quorum :: pos_integer()
  @type timeout_in_ms :: :infinity | non_neg_integer()
  @type timestamp_in_ms :: non_neg_integer()

  @type range_tag :: non_neg_integer()

  @type service :: :coordination | :log | :storage
  @type service_id :: String.t()
end
