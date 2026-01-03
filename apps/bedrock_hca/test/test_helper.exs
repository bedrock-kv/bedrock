ExUnit.start()

# Cleanup tmp directory after test suite completes
ExUnit.after_suite(fn _ -> File.rm_rf("tmp") end)

if !Code.ensure_loaded?(MockRepo) do
  Mox.defmock(MockRepo, for: Bedrock.Repo)
end

Mox.stub(MockRepo, :transact, fn callback -> callback.() end)
