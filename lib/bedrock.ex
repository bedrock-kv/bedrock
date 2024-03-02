defmodule Bedrock do
  @type key :: binary()
  @type value :: binary()
  @type key_value :: {key(), value()}
  @type epoch :: non_neg_integer()
  @type timeout_in_ms :: :infinity | non_neg_integer()

  defmacro __using__(:types) do
    quote do
      @type key :: Bedrock.key()
      @type value :: Bedrock.value()
      @type key_value :: Bedrock.key_value()
      @type epoch :: Bedrock.epoch()
      @type timeout_in_ms :: Bedrock.timeout_in_ms()
    end
  end
end
