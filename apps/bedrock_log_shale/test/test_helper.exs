ExUnit.start()

# Cleanup tmp directory after test suite completes
ExUnit.after_suite(fn _ -> File.rm_rf("tmp") end)
