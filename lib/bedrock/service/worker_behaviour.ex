defmodule Bedrock.Service.WorkerBehaviour do
  @callback one_time_initialization(Path.t()) :: :ok | {:error, term()}

  defmacro __using__(_) do
    quote do
      @behaviour Bedrock.Service.WorkerBehaviour

      @impl Bedrock.Service.WorkerBehaviour
      def one_time_initialization(_path), do: :ok

      defoverridable one_time_initialization: 1
    end
  end
end
