defmodule Bedrock.Internal.GenServerApi do
  defmacro __using__(opts) do
    module = Keyword.get(opts, :for)

    quote do
      unquote do
        if module do
          quote do
            @doc false
            defdelegate child_spec(opts), to: unquote(module)
          end
        end
      end

      @type call_errors :: :unavailable | :timeout

      import Bedrock.Internal.GenServer.Calls
    end
  end
end
