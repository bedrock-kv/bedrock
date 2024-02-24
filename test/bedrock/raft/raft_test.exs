defmodule Bedrock.RaftTest do
  use ExUnit.Case, async: true

  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.Mode.Leader
  alias Bedrock.Raft.Mode.Follower
  alias Bedrock.Raft.Mode.Candidate

  import Mox
  alias Bedrock.Raft.MockInterface
  setup :verify_on_exit!

  def mock_timer_cancel, do: :ok

  describe "Raft can be constructed" do
    test "when creating an instance with no other nodes, we start out as a leader" do
      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 0} -> :ok end)

      p = Raft.new(:a, [], InMemoryLog.new(), MockInterface)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [],
                 quorum: 0,
                 term: 0,
                 pongs: []
               },
               nodes: [],
               quorum: 0
             } = p

      assert :a = Raft.me(p)
      assert {:a, 0} = Raft.leadership(p)
      assert Raft.am_i_the_leader?(p)
      assert [:a] = Raft.known_nodes(p)
    end

    test "when creating an instance with other nodes, we start out as a follower" do
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)

      p = Raft.new(:a, [:b], InMemoryLog.new(), MockInterface)

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 0,
                 leader: :undecided,
                 voted_for: nil,
                 cancel_timer_fn: cancel_timer_fn
               },
               nodes: [:b],
               quorum: 1
             } = p

      assert cancel_timer_fn == (&mock_timer_cancel/0)

      assert :a = Raft.me(p)
      assert {:undecided, 0} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)
      assert [:a, :b] = Raft.known_nodes(p)
    end

    test "when creating an instance with more than zero other nodes, we start out as a follower" do
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)

      p = Raft.new(:a, [:b], InMemoryLog.new(), MockInterface)

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 0,
                 leader: :undecided,
                 voted_for: nil,
                 cancel_timer_fn: cancel_timer_fn
               },
               nodes: [:b],
               quorum: 1
             } = p

      assert cancel_timer_fn == (&mock_timer_cancel/0)

      assert :a = Raft.me(p)
      assert {:undecided, 0} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)
      assert [:a, :b] = Raft.known_nodes(p)
    end
  end

  describe "Raft.Raft elections" do
    test "A happy-path election between three nodes that we started" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)

      p = Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)

      # We haven't yet heard from any other nodes, so there's definitely no
      # leader.
      assert {:undecided, 0} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)

      # Assuming that node :a's election timer expires, it will become a
      # candidate and start an election for term 2. It will send a request_vote
      # message to :b and :c.
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)

      p = Raft.handle_event(p, :election, :timer)

      assert %Raft{
               me: :a,
               mode: %Candidate{
                 term: 1,
                 quorum: 1,
                 nodes: [:b, :c],
                 votes: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # While we are a candidate, there's definitely no leader.
      assert {:undecided, 1} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)

      # Assuming that node :b receives the request_vote message first, it will
      # respond with a vote for term 2. With one vote, we reach the quorum and
      # become leader.
      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)

      p = Raft.handle_event(p, {:vote, 1}, :b)

      # After we've won the election, we should be the leader.
      assert {:a, 1} = Raft.leadership(p)
      assert Raft.am_i_the_leader?(p)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1,
                 pongs: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Assuming that node :c also responds to the request_vote, we've already
      # reached the quorum and become leader. We'll ignore the vote.
      expect(MockInterface, :ignored_event, fn {:vote, 1}, :c -> :ok end)

      p = Raft.handle_event(p, {:vote, 1}, :c)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1,
                 pongs: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Assuming that node :c receives the ping and responds with a pong,
      # ensure that it is recorded properly.
      p = Raft.handle_event(p, {:append_entries_ack, 1, {0, 0}}, :c)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1,
                 pongs: [:c]
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # At the next heartbeat timer, assuming that node :c receives the ping and
      # responds with a pong, ensure that it is recorded properly.
      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p = Raft.handle_event(p, :heartbeat, :timer)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1,
                 pongs: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p
    end

    test "A happy-path election between three nodes that we vote in" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)

      p = Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)

      # Node :c calls an election for term 1, and sends a request_vote message
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:vote, 1} -> :ok end)

      p = Raft.handle_event(p, {:request_vote, 1, {0, 1}}, :c)

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 1,
                 leader: :undecided,
                 voted_for: :c
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Node :c calls to inform that the election has been decided for term 1,
      # and that it has been elected.
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 1, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 1} -> :ok end)

      p = Raft.handle_event(p, {:append_entries, 1, t0, [], t0}, :c)

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 1,
                 leader: :c,
                 voted_for: :c
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      assert {:c, 1} = Raft.leadership(p)
    end

    test "After becoming a candidate, if the election timer expires then a new election is started" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)

      p = Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)

      # Timer elapses the first time, so we start an election for term 2
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)

      p = Raft.handle_event(p, :election, :timer)

      assert %Raft{
               me: :a,
               mode: %Candidate{
                 term: 1,
                 quorum: 1,
                 nodes: [:b, :c],
                 votes: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Timer elapses the second time with no votes, so we start an election for
      # term 3

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 2} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 2, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 2, ^t0} -> :ok end)

      p = Raft.handle_event(p, :election, :timer)

      assert %Raft{
               me: :a,
               mode: %Candidate{
                 term: 2,
                 quorum: 1,
                 nodes: [:b, :c],
                 votes: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p
    end

    test "In a three node cluster, as a candidate we receive a vote from a follower in an older term" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      assert %Raft{mode: %Candidate{}} = p
      assert {:undecided, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 3.

      p = p |> Raft.handle_event({:vote, 0}, :c)

      assert {:undecided, 1} = Raft.leadership(p)
    end

    test "In a three node cluster, as a candidate we receive a ping from a leader in the same term" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      assert %Raft{mode: %Candidate{}} = p
      assert {:undecided, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 3.

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 2} -> :ok end)

      p = p |> Raft.handle_event({:append_entries, 2, t0, [], t0}, :c)

      assert {:c, 2} = Raft.leadership(p)
    end

    test "In a three node cluster, as a candidate we receive a ping from a leader in an older term" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      assert %Raft{mode: %Candidate{}} = p
      assert {:undecided, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the leader of term 1.

      p = p |> Raft.handle_event({:append_entries, 0, t0, [], t0}, :c)

      assert {:undecided, 1} = Raft.leadership(p)
    end

    test "In a three node cluster, after winning a successful election during a network split, a new leader was elected" do
      t0 = {0, 0}
      t1 = {2, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)
      expect(MockInterface, :ignored_event, fn {:vote, 1}, :b -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)
        |> Raft.handle_event(:heartbeat, :timer)

      assert {:a, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 2.

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t1} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 2} -> :ok end)

      p = p |> Raft.handle_event({:append_entries, 2, t0, [{t1, :data1}], t1}, :c)

      assert {:c, 2} = Raft.leadership(p)
    end
  end

  describe "Raft.Raft quorum failures" do
    test "A three node cluster where we are the leader, and quorum fails" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)
      expect(MockInterface, :ignored_event, fn {:vote, 1}, :b -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1,
                 pongs: [:b, :c]
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      assert {:a, 1} = Raft.leadership(p)

      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      # some time goes by, and we don't hear back from either :b or :c...

      p =
        p
        |> Raft.handle_event(:heartbeat, :timer)

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 2} -> :ok end)

      p = p |> Raft.handle_event(:heartbeat, :timer)

      assert {:undecided, 2} = Raft.leadership(p)

      assert {:error, :not_leader} = p |> Raft.add_transaction({{2, 0}, :data2})

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 2,
                 leader: :undecided,
                 voted_for: nil
               },
               nodes: [:b, :c],
               quorum: 1
             } = p
    end
  end

  describe "Raft.Raft log interaction" do
    test "A three node cluster where we become the leader, and then we append an entry" do
      t0 = {0, 0}
      t1 = {1, 0}
      t2 = {1, 1}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)
      expect(MockInterface, :ignored_event, fn {:vote, 1}, :b -> :ok end)

      p =
        p
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)

      p =
        p
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)

      assert p |> Raft.am_i_the_leader?()

      assert {:ok, p} = Raft.add_transaction(p, {t1, :data1})
      assert {:ok, p} = Raft.add_transaction(p, {t2, :data2})

      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)

      expect(MockInterface, :send_event, fn :b,
                                            {:append_entries, 1, ^t0,
                                             [{^t1, :data1}, {^t2, :data2}], ^t0} ->
        :ok
      end)

      expect(MockInterface, :send_event, fn :c,
                                            {:append_entries, 1, ^t0,
                                             [{^t1, :data1}, {^t2, :data2}], ^t0} ->
        :ok
      end)

      p =
        p
        |> Raft.handle_event(:heartbeat, :timer)

      assert [{t1, :data1}, {t2, :data2}] == p |> Raft.log() |> Log.transactions_from(t0, :newest)
      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t0 == p |> Raft.log() |> Log.newest_safe_transaction_id()

      expect(MockInterface, :timer, fn :heartbeat, 50, 50 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t2, [], ^t2} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t2, [], ^t2} -> :ok end)

      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t0 == p |> Raft.log() |> Log.newest_safe_transaction_id()

      p =
        p
        |> Raft.handle_event({:append_entries_ack, 1, t2}, :c)
        |> Raft.handle_event(:heartbeat, :timer)

      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t2 == p |> Raft.log() |> Log.newest_safe_transaction_id()
    end

    test "A three node cluster where we are a follower, where we start off at nothing and need to catch up" do
      t0 = {0, 0}
      t1 = {2, 0}
      t2 = {2, 1}

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:undecided, 0} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)

      # some time passes, and we hear a from an active leader.

      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 1, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 1} -> :ok end)

      p =
        p
        |> Raft.handle_event({:append_entries, 1, t1, [], t1}, :c)

      # the leader responds with a new set of transactions, and we append them to
      # our log.
      expect(MockInterface, :timer, fn :election, 150, 300 -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 1, ^t2} -> :ok end)

      p =
        p
        |> Raft.handle_event({:append_entries, 1, t0, [{t1, :data1}, {t2, :data2}], t2}, :c)

      assert [{t1, :data1}, {t2, :data2}] == p |> Raft.log() |> Log.transactions_from(t0, :newest)
      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t2 == p |> Raft.log() |> Log.newest_safe_transaction_id()
    end
  end
end
