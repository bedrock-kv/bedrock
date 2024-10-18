defmodule Bedrock.DataPlane.Sequencer.Server do
  alias Bedrock.DataPlane.Sequencer.State

  import Bedrock.DataPlane.Sequencer.State,
    only: [
      update_last_committed_version: 2
    ]

  import Bedrock.DataPlane.Sequencer.Versions,
    only: [
      next_version: 1,
      monotonic_time_in_ms: 0
    ]

  import Bedrock.DataPlane.Sequencer.ControllerFeedback,
    only: [
      accept_invitation: 1,
      decline_invitation: 1
    ]

  use GenServer

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    controller = opts[:controller] || raise "Missing :controller option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    started_at = opts[:started_at] || raise "Missing :started_at option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, controller, epoch, started_at},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({cluster, controller, epoch, system_time_started_at_in_ms, last_committed_version}) do
    %State{
      cluster: cluster,
      controller: controller,
      epoch: epoch,
      last_committed_version: last_committed_version
    }
    |> then(&{:ok, &1, {:continue, {:start_clock, system_time_started_at_in_ms}}})
  end

  @impl true
  def handle_continue({:start_clock, system_time_started_at_in_ms}, t) do
    now = monotonic_time_in_ms()

    %{
      t
      | started_at: now - (:os.system_time(:millisecond) - system_time_started_at_in_ms),
        last_timestamp_at: now,
        sequence: 0
    }
    |> noreply()
  end

  @impl true
  def handle_cast(
        {:recruitment_invitation, controller, epoch, system_time_started_at_in_ms,
         last_committed_version},
        t
      )
      when t.epoch > epoch do
    %State{
      cluster: t.cluster,
      controller: controller,
      epoch: epoch,
      last_committed_version: last_committed_version
    }
    |> accept_invitation()
    |> noreply({:start_clock, system_time_started_at_in_ms})
  end

  def handle_cast({:recruitment_invitation, _controller, _epoch}, t),
    do: t |> decline_invitation() |> noreply()

  @impl GenServer
  def handle_call(:next_read_version, _from, t),
    do: t |> reply({:ok, t.latest_committed_version})

  def handle_call(:next_commit_version, _from, t) do
    {next_version, t} = next_version(t)

    t
    |> update_last_committed_version(next_version)
    |> reply({:ok, {t.latest_committed_version, next_version}})
  end

  @impl GenServer
  def handle_info(:die, _t), do: raise("die!")

  defp noreply(t), do: {:noreply, t}
  defp noreply(t, continue), do: {:noreply, t, {:continue, continue}}
  defp reply(t, result), do: {:reply, result, t}
end
