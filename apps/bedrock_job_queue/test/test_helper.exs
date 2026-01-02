ExUnit.start()

# Define MockRepo using Mox for the Bedrock.Repo behaviour
Mox.defmock(MockRepo, for: Bedrock.Repo)
