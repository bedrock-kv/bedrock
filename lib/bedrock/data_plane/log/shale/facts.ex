defmodule Bedrock.DataPlane.Log.Shale.Facts do
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log

  @spec info(State.t(), Log.fact_name() | [Log.fact_name()]) ::
          {:ok, term() | %{Log.fact_name() => term()}} | {:error, :unsupported_info}
  def info(%State{} = t, fact) when is_atom(fact), do: {:ok, gather_info(fact, t)}

  def info(%State{} = t, facts) when is_list(facts) do
    {:ok,
     facts
     |> Map.new(fn
       fact_name -> {fact_name, gather_info(fact_name, t)}
     end)}
  end

  defp supported_info,
    do: [
      :id,
      :kind,
      :minimum_durable_version,
      :oldest_version,
      :last_version,
      :otp_name,
      :pid,
      :state,
      :supported_info
    ]

  @spec gather_info(Log.fact_name(), any()) :: term() | {:error, :unsupported}
  # Worker facts
  defp gather_info(:id, %{id: id}), do: id
  defp gather_info(:state, %{state: state}), do: state
  defp gather_info(:kind, _t), do: :log
  defp gather_info(:otp_name, %State{otp_name: otp_name}), do: otp_name
  defp gather_info(:pid, _), do: self()
  defp gather_info(:supported_info, _), do: supported_info()

  # Transaction Log facts
  defp gather_info(:minimum_durable_version, _t), do: :unavailable

  defp gather_info(:oldest_version, t), do: t.oldest_version
  defp gather_info(:last_version, t), do: t.last_version

  # Everything else...
  defp gather_info(_, _), do: {:error, :unsupported}
end
