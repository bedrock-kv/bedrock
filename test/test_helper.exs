ExUnit.start()
Faker.start()

# Start recovery telemetry tracing for all tests
unless Process.whereis(:bedrock_trace_director_recovery) do
  Bedrock.ControlPlane.Director.Recovery.Tracing.start()
end

Mox.defmock(Bedrock.Raft.MockInterface, for: Bedrock.Raft.Interface)
