defmodule Bedrock.Internal.Tracing.RaftTelemetry do
  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_raft_telemetry"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :raft, :mode_change],
        [:bedrock, :raft, :consensus_reached],
        [:bedrock, :raft, :leadership_change],
        [:bedrock, :raft, :request_votes],
        [:bedrock, :raft, :vote_received],
        [:bedrock, :raft, :vote_sent],
        [:bedrock, :raft, :election_ended],
        [:bedrock, :raft, :transaction_added],
        [:bedrock, :raft, :append_entries_ack_received],
        [:bedrock, :raft, :append_entries_ack_sent],
        [:bedrock, :raft, :heartbeat],
        [:bedrock, :raft, :append_entries_sent],
        [:bedrock, :raft, :append_entries_received]
      ],
      &__MODULE__.log_event/4,
      :os.system_time(:millisecond)
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  def to_hh_mm_ss_ms(0), do: "0:00.000"

  def to_hh_mm_ss_ms(milliseconds) do
    units = [3_600_000, 60_000, 1000, 1]
    digits = [2, 2, 3]

    [h | t] =
      Enum.map_reduce(
        units,
        milliseconds,
        fn
          unit, val ->
            {div(val, unit), rem(val, unit)}
        end
      )
      |> elem(0)

    {h, t} = if Enum.empty?(t), do: {0, [h]}, else: {h, t}

    "#{h}:#{t |> Enum.zip(digits) |> Enum.map_join(":", fn {x, n} -> x |> Integer.to_string() |> String.pad_leading(n, "0") end)}"
  end

  defp info(elapsed, message), do: Logger.info("#{to_hh_mm_ss_ms(elapsed)}: #{message}")

  @spec log_event(list(atom()), map(), map(), integer()) :: :ok
  def log_event(
        [:bedrock, :raft, :mode_change],
        %{at: at},
        %{mode: :follower, term: term, leader: leader},
        start
      ) do
    info(
      at - start,
      "Became follower with term #{term} and leader #{leader}"
    )
  end

  def log_event(
        [:bedrock, :raft, :mode_change],
        %{at: at},
        %{mode: :candidate, term: term, quorum: quorum, peers: peers},
        start
      ) do
    info(
      at - start,
      "Became candidate with term #{term}, quorum #{quorum}, and peers #{inspect(peers)}"
    )
  end

  def log_event(
        [:bedrock, :raft, :mode_change],
        %{at: at},
        %{mode: :leader, term: term, quorum: quorum, peers: peers},
        start
      ) do
    info(
      at - start,
      "Became leader with term #{term}, quorum #{quorum}, and peers #{inspect(peers)}"
    )
  end

  def log_event(
        [:bedrock, :raft, :consensus_reached],
        %{at: at},
        %{transaction_id: transaction_id},
        start
      ) do
    info(at - start, "Consensus reached for transaction #{inspect(transaction_id)}")
  end

  def log_event(
        [:bedrock, :raft, :leadership_change],
        %{at: at},
        %{leader: leader, term: term},
        start
      ) do
    info(at - start, "Leadership changed to #{leader} with term #{term}")
  end

  def log_event(
        [:bedrock, :raft, :request_votes],
        %{at: at},
        %{term: term, peers: peers, newest_transaction_id: newest_transaction_id},
        start
      ) do
    info(
      at - start,
      "Requesting votes for term #{term} from peers #{inspect(peers)} with newest transaction ID #{inspect(newest_transaction_id)}"
    )
  end

  def log_event(
        [:bedrock, :raft, :vote_received],
        %{at: at},
        %{term: term, follower: follower},
        start
      ) do
    info(at - start, "Received vote for from #{follower} in term #{term}")
  end

  def log_event(
        [:bedrock, :raft, :vote_sent],
        %{at: at},
        %{term: term, candidate: candidate},
        start
      ) do
    info(at - start, "Voted for candidate #{candidate} in term #{term}")
  end

  def log_event(
        [:bedrock, :raft, :election_ended],
        %{at: at},
        %{term: term, votes: votes, quorum: quorum},
        start
      ) do
    info(
      at - start,
      "Election ended for term #{term} with votes #{inspect(votes)} and quorum #{quorum}"
    )
  end

  def log_event(
        [:bedrock, :raft, :transaction_added],
        %{at: at},
        %{term: term, transaction_id: transaction_id},
        start
      ) do
    info(at - start, "Transaction #{inspect(transaction_id)} added to term #{term}")
  end

  def log_event(
        [:bedrock, :raft, :append_entries_ack_received],
        %{at: at},
        %{term: term, follower: follower, newest_transaction_id: newest_transaction_id} =
          _metadata,
        start
      ) do
    info(
      at - start,
      "Received append entries ack for term #{term} from follower #{follower} with newest transaction ID #{inspect(newest_transaction_id)}"
    )
  end

  def log_event(
        [:bedrock, :raft, :append_entries_ack_sent],
        %{at: at},
        %{term: term, leader: leader, newest_transaction_id: newest_transaction_id} =
          _metadata,
        start
      ) do
    info(
      at - start,
      "Sent append entries ack for term #{term} to leader #{leader} with newest transaction ID #{inspect(newest_transaction_id)}"
    )
  end

  def log_event(
        [:bedrock, :raft, :heartbeat],
        %{at: at},
        %{term: term},
        start
      ) do
    info(at - start, "Heartbeat for term #{term}")
  end

  def log_event(
        [:bedrock, :raft, :append_entries_sent],
        %{at: at},
        %{
          follower: follower,
          term: term,
          prev_transaction_id: prev_transaction_id,
          transaction_ids: transaction_ids,
          newest_safe_transaction_id: newest_safe_transaction_id
        },
        start
      ) do
    info(
      at - start,
      "Sent append entries for term #{term} from follower #{follower} with prev transaction ID #{inspect(prev_transaction_id)}, transaction IDs #{inspect(transaction_ids)}, and newest safe transaction ID #{inspect(newest_safe_transaction_id)}"
    )
  end

  def log_event(
        [:bedrock, :raft, :append_entries_received],
        %{at: at},
        %{
          term: term,
          leader: leader,
          prev_transaction_id: prev_transaction_id,
          transaction_ids: transaction_ids,
          commit_transaction_id: commit_transaction_id
        },
        start
      ) do
    info(
      at - start,
      "Received append entries for term #{term} from leader #{leader} with prev transaction ID #{inspect(prev_transaction_id)}, transaction IDs #{inspect(transaction_ids)}, and commit transaction ID #{inspect(commit_transaction_id)}"
    )
  end

  # def log_event(_, _, _, _), do: :ok
end
