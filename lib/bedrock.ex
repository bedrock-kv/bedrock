defmodule Bedrock do
  @type key :: binary()
  @type value :: binary()
  @type key_value :: {key(), value()}
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key() | nil}
  @type epoch :: non_neg_integer()
  @type timeout_in_ms :: :infinity | non_neg_integer()

  @type tag :: non_neg_integer()

  @type service :: :coordination | :log | :storage
  @type service_id :: String.t()
end
