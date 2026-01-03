# Centralized version management for the Bedrock umbrella project.
#
# Publish order (dependencies first):
#   1. bedrock_core
#   2. bedrock_log_shale, bedrock_storage_olivine (can be parallel)
#   3. bedrock_hca
#   4. bedrock_directory
#   5. bedrock (convenience package)

%{
  bedrock: "0.5.0",
  bedrock_core: "0.4.0",
  bedrock_directory: "0.1.0",
  bedrock_hca: "0.1.0",
  bedrock_job_queue: "0.1.0",
  bedrock_log_shale: "0.1.0",
  bedrock_storage_olivine: "0.1.0"
}
