defmodule Bedrock do
  @type key :: binary()
  @type value :: binary()
  @type key_value :: {key(), value()}

  defmacro __using__(:types) do
    quote do
      @type key :: Bedrock.key()
      @type value :: Bedrock.value()
      @type key_value :: Bedrock.key_value()
    end
  end
end
