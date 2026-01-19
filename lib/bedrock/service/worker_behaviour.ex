defmodule Bedrock.Service.WorkerBehaviour do
  @moduledoc false

  @callback kind() :: :log | :materializer
  @callback one_time_initialization(Path.t()) :: :ok | {:error, File.posix()}

  defmacro __using__(opts) do
    kind = opts[:kind] || raise "Must declare a :kind"

    if kind not in [:log, :materializer] do
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
