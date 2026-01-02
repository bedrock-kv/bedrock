ExUnit.start()

# Define MockRepo using Mox for the Bedrock.Repo behaviour
# Use conditional to avoid redefinition warning in umbrella tests
if !Code.ensure_loaded?(MockRepo) do
  Mox.defmock(MockRepo, for: Bedrock.Repo)
end
