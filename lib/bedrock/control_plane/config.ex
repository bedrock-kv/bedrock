defmodule Bedrock.ControlPlane.Config do
  @moduledoc """
  This module defines the configuration structure and functions for the Bedrock
  Control Plane. It provides functions for accessing and manipulating the
  configuration parameters, policies and transaction system layout.
  """
  alias Bedrock.ControlPlane.Config.Policies
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: %__MODULE__{
          state: state(),
          parameters: map(),
          policies: Policies.t(),
          transaction_system_layout: TransactionSystemLayout.t()
        }
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

  @type state :: :initializing | :running | :stopping

  @doc """
  Creates a new configuration struct.
  """
  @spec new(state(), parameters :: map(), Policies.t(), TransactionSystemLayout.t()) :: t()
  def new(state, parameters, policies, transaction_system_layout) do
    %__MODULE__{
      state: state,
      parameters: parameters,
      policies: policies,
      transaction_system_layout: transaction_system_layout
    }
  end

  @doc """
  Returns true if the cluster will allow volunteer nodes to join.
  """
  @spec allow_volunteer_nodes_to_join?(t()) :: boolean()
  def allow_volunteer_nodes_to_join?(t), do: t.policies.allow_volunteer_nodes_to_join

  @doc """
  Returns the nodes that are part of the cluster.
  """
  @spec nodes(t()) :: [node()]
  def nodes(t), do: t.parameters[:nodes] || []

  @doc """
  Returns the ping rate in milliseconds.
  """
  @spec ping_rate_in_ms(t()) :: non_neg_integer()
  def ping_rate_in_ms(t), do: div(1000, t.parameters.ping_rate_in_hz)

  @doc """
  Returns the pid of the sequencer.
  """
  @spec sequencer(t()) :: pid() | nil
  def sequencer(t), do: find_singleton_service(t, :sequencer)

  @doc """
  Returns the pid of the data distributor.
  """
  @spec data_distributor(t()) :: pid() | nil
  def data_distributor(t), do: find_singleton_service(t, :data_distributor)

  @doc """
  Returns the otp_names of the log workers.
  """
  @spec log_workers(t()) :: [atom()]
  def log_workers(t), do: find_multiple_services(t, :log)

  @doc """
  Returns the otp_names of the storage workers.
  """
  @spec storage_workers(t()) :: [atom()]
  def storage_workers(t), do: find_multiple_services(t, :storage)

  @doc """
  Returns the entire service directory.
  """
  def service_directory(%__MODULE__{
        transaction_system_layout: %TransactionSystemLayout{
          service_directory: service_directory
        }
      }),
      do: service_directory

  @spec find_singleton_service(t(), atom()) :: atom() | nil
  defp find_singleton_service(%__MODULE__{} = t, service_type) do
    t
    |> service_directory()
    |> Enum.find(nil, &match?(%{type: ^service_type}, &1))
    |> case do
      nil -> nil
      %{otp_name: otp_name} -> otp_name
    end
  end

  @spec find_multiple_services(t(), atom()) :: [atom()]
  defp find_multiple_services(%__MODULE__{} = t, service_type) do
    t
    |> service_directory()
    |> Enum.filter(&match?(%{type: ^service_type}, &1))
    |> Enum.map(& &1.otp_name)
  end
end
