defmodule Bedrock.ControlPlane.Config do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: %__MODULE__{}
  defstruct [
    # The current state of the cluster.
    state: :initializing,

    # The parameters that are used to configure the cluster.
    parameters: nil,

    # The policies that are used to configure the cluster.
    policies: nil,

    # The layout of the transaction system.
    transaction_system_layout: nil
  ]

  @spec allow_volunteer_nodes_to_join?(t()) :: boolean()
  def allow_volunteer_nodes_to_join?(t), do: t.policies.allow_volunteer_nodes_to_join

  @spec nodes(t()) :: [node()]
  def nodes(t), do: t.parameters.nodes

  @spec ping_rate_in_ms(t()) :: non_neg_integer()
  def ping_rate_in_ms(t), do: div(1000, t.parameters.ping_rate_in_hz)

  @spec sequencer(t()) :: pid() | nil
  def sequencer(t), do: find_singleton_service(t, :sequencer)

  @spec data_distributor(t()) :: pid() | nil
  def data_distributor(t), do: find_singleton_service(t, :data_distributor)

  @spec log_workers(t()) :: [pid()]
  def log_workers(t), do: find_multiple_services(t, :log)

  def service_directory(%__MODULE__{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: service_directory
        }
      }),
      do: service_directory

  @spec find_singleton_service(t(), atom()) :: pid() | nil
  defp find_singleton_service(%__MODULE__{} = t, service_type) do
    t |> service_directory() |> Enum.find(nil, &match?(%{type: ^service_type}, &1.otp_name))
  end

  @spec find_multiple_services(t(), atom()) :: [pid()]
  defp find_multiple_services(%__MODULE__{} = t, service_type) do
    t |> service_directory() |> Enum.filter(&match?(%{type: ^service_type}, &1.otp_name))
  end
end
