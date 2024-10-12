defmodule Bedrock.Internal.ChildSpec do
  defmacro __using__(opts) do
    module = Keyword.get(opts, :for, Module.concat([__CALLER__.module, Server]))

    quote do
      @doc false
      @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
      defdelegate child_spec(opts), to: unquote(module)
    end
  end
end
