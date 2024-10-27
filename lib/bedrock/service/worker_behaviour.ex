defmodule Bedrock.Service.WorkerBehaviour do
  @callback kind() :: :log | :storage
  @callback one_time_initialization(Path.t()) :: :ok | {:error, term()}

  defmacro __using__(opts) do
    kind = opts[:kind] || raise "Must declare a :kind"

    if kind not in [:log, :storage] do
      raise "Invalid :kind: #{inspect(kind)}"
    end

    quote do
      @behaviour Bedrock.Service.WorkerBehaviour

      @impl true
      def kind, do: unquote(kind)

      @impl true
      def one_time_initialization(_path), do: :ok

      defoverridable one_time_initialization: 1
    end
  end
end
