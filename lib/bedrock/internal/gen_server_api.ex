defmodule Bedrock.Internal.GenServerApi do
  defmacro __using__(opts) do
    module = Keyword.get(opts, :for)

    quote do
      unquote do
        if module do
          quote do
            @doc false
            @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
            defdelegate child_spec(opts), to: unquote(module)
          end
        end
      end

      import Bedrock.Internal.GenServer.Calls
    end
  end
end
