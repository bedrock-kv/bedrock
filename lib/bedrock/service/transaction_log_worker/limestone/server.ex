defmodule Bedrock.Service.TransactionLogWorker.Limestone.Server do
  use GenServer

  defmodule State do
    @type t :: %__MODULE__{}
    defstruct [:id, :otp_name]
  end

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)
    otp_name = Keyword.fetch!(opts, :otp_name)

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {id, otp_name},
           [name: otp_name]
         ]}
    }
  end

  @impl GenServer
  def init({id, otp_name}) do
    {:ok, %State{id: id, otp_name: otp_name}}
  end

  @impl GenServer
  def handle_call({:info, fact_names}, _from, t) do
    {:reply, t |> info(fact_names), t}
  end

  @spec info(State.t(), atom() | [atom()]) :: {:ok, any()} | {:error, :unsupported_info}
  def info(%State{} = t, fact) when is_atom(fact), do: {:ok, gather_info(fact, t)}

  def info(%State{} = t, facts) when is_list(facts) do
    {:ok,
     facts
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
     end)}
  end

  defp supported_info, do: ~w[
      id
      kind
      pid
      otp_name
      supported_info
    ]a

  @spec gather_info(fact_name :: atom(), any()) :: term() | {:error, :unsupported}
  defp gather_info(:id, %{id: id}), do: id
  defp gather_info(:kind, _t), do: :transaction_log
  defp gather_info(:otp_name, %State{otp_name: otp_name}), do: otp_name
  defp gather_info(:pid, _), do: self()
  defp gather_info(:supported_info, _), do: supported_info()
  defp gather_info(_, _), do: {:error, :unsupported}
end
