defmodule Bedrock.Internal.GenServerApi do
  @moduledoc false

  defmacro __using__(opts) do
    module = Keyword.get(opts, :for)

    quote do
      unquote do
        if module do
          quote do
            @doc false
            defdelegate child_spec(opts), to: unquote(module)

            def start_link(opts) do
              GenServer.start_link(unquote(module), opts)
            end
          end
        end
      end

      import Bedrock.Internal.GenServer.Calls
    end
  end
end
