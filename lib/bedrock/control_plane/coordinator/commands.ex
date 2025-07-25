defmodule Bedrock.ControlPlane.Coordinator.Commands do
  @moduledoc """
  Structured command types for Raft consensus operations.

  All coordinator operations that require consensus should use these
  structured commands instead of raw data.
  """

  alias Bedrock.ControlPlane.Config

  @type command ::
          start_epoch_command()
          | update_config_command()

  @type start_epoch_command ::
          {:start_epoch,
           %{
             epoch: Bedrock.epoch(),
             director: pid(),
             relieving: {Bedrock.epoch(), pid()} | {Bedrock.epoch(), :unavailable}
           }}

  @type update_config_command ::
          {:update_config,
           %{
             config: Config.t()
           }}

  @doc """
  Create a command to start a new epoch with its director via consensus.
  """
  @spec start_epoch(Bedrock.epoch(), pid(), {Bedrock.epoch(), pid() | :unavailable}) ::
          start_epoch_command()
  def start_epoch(epoch, director, relieving),
    do: {
      :start_epoch,
      %{
        epoch: epoch,
        director: director,
        relieving: relieving
      }
    }

  @doc """
  Create a command to update cluster configuration via consensus.
  """
  @spec update_config(Config.t()) :: update_config_command()
  def update_config(config),
    do: {
      :update_config,
      %{config: config}
    }
end
