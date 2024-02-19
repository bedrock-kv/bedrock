defmodule Bedrock.DataPlane.LogSystem.Engine do
  use Bedrock.Cluster, :types

  @type t :: Bedrock.Engine.t()

  @type timeout_in_ms :: :infinity | non_neg_integer()

  @type basic_fact_name ::
          :id
          | :kind
          | :otp_name
          | :path
          | :supported_info

  @doc """
  Ask the transaction log engine for various facts about itself.
  """
  @spec info(t(), [basic_fact_name()]) :: {:ok, keyword()} | {:error, term()}
  @spec info(t(), [basic_fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, term()}
  defdelegate info(engine, fact_names, timeout \\ 5_000), to: Bedrock.Worker
end
