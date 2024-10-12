defmodule Bedrock.Cluster.PubSub do
  @moduledoc """
  A simple pub/sub implementation using the `Registry` module for dispatching
  messages within a single cluster node.
  """

  @doc false
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    %{
      id: __MODULE__,
      start: {
        Registry,
        :start_link,
        [
          [keys: :duplicate, partitions: System.schedulers_online(), name: otp_name]
        ]
      },
      restart: :permanent
    }
  end

  @doc """
  Publish a message to a topic. This will send the message to all subscribing
  processes.
  """
  @spec publish(cluster :: module(), topic :: any(), message :: any()) :: :ok
  def publish(cluster, topic, message) do
    cluster.otp_name(:pub_sub)
    |> Registry.dispatch(topic, fn
      handlers ->
        handlers
        |> Enum.each(fn
          {pid, _} ->
            send(pid, message)
        end)
    end)
  end

  @doc """
  Subscribe to a topic. This will register the current process as a subscriber
  to the given topic. The current process will receive future messages published
  for the given topic.
  """
  @spec subscribe(cluster :: module(), topic :: any()) :: :ok
  def subscribe(cluster, topic) do
    cluster.otp_name(:pub_sub)
    |> Registry.register(topic, self())

    :ok
  end

  @doc """
  Unsubscribe from a topic. This will unregister the current process as a
  subscriber to the given topic. The current process will no longer receive
  messages for the given topic.
  """
  @spec unsubscribe(cluster :: module(), topic :: any()) :: :ok
  def unsubscribe(cluster, topic) do
    cluster.otp_name(:pub_sub)
    |> Registry.unregister_match(topic, self())
  end
end
