defmodule Bedrock.Service.TransactionLogWorker.Limestone.TransactionReceiver do
  use GenServer

  alias Bedrock.Service.TransactionLogWorker.Limestone.Transactions
  # alias Bedrock.Service.TransactionLogWorker.Limestone.SegmentRecycler

  defstruct [:transactions]
  @type t :: %__MODULE__{}

  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    recycler = opts[:recycler] || raise "Missing :recycler option"
    transactions = opts[:transactions] || raise "Missing :transactions option"
    controller = opts[:controller] || raise "Missing :controller option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             transactions,
             recycler,
             controller
           },
           [name: otp_name]
         ]},
      restart: :transient
    }
  end

  def init({transactions, _recycler, _controller}) do
    #    {:ok, _segment} = SegmentRecycler.check_out(recycler, "/tmp/test")

    {:ok, %__MODULE__{transactions: transactions}}
  end

  def handle_call({:append, transaction}, _from, state) do
    state.transactions |> Transactions.append!(transaction)
    {:reply, :ok, state, {:continue, :notify_reader_and_writer}}
  end

  def handle_continue(:notify_reader_and_writer, state) do
    {:noreply, state}
  end
end
