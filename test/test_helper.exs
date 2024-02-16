ExUnit.start()
Faker.start()

Mox.defmock(Bedrock.Raft.MockInterface, for: Bedrock.Raft.Interface)
