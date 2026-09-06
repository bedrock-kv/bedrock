if !Node.alive?() do
  raise "Run with MIX_ENV=test elixir --sname bedrock_history -S mix run --no-start scripts/transaction_history.exs"
end

{:ok, _} = Application.ensure_all_started(:bedrock)
seed = String.to_integer(System.get_env("BEDROCK_HISTORY_SEED", "239"))
ExUnit.start(seed: seed, max_cases: 1)

for suite <- ~w(history_oracle transaction_history snapshot_history peer_history) do
  Code.require_file("test/bedrock/distributed/#{suite}_test.exs")
end
