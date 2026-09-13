# Focused local suite without booting unrelated MinIO fixtures.
ExUnit.start(autorun: false)
paths = Path.wildcard("test/bedrock/object_storage/local_filesystem*_test.exs") ++
  ["test/bedrock/object_storage/error_contract_test.exs", "test/bedrock/object_storage/listing_truthfulness_test.exs"]
Enum.each(paths, &Code.require_file/1)
result = ExUnit.run()
System.halt(if(result.failures == 0, do: 0, else: 1))
