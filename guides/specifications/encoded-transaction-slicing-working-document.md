# Working Document: Enhanced EncodedTransaction Slicing for Storage Team Tags

## Summary

Investigation into existing EncodedTransaction functionality and requirements for slicing encoded transactions by storage team tags to improve commit proxy efficiency.

## Decisions

- Storage team tags are used to partition keys across different storage teams
- Each storage team has a tag and a key range it's responsible for  
- The commit proxy currently groups transactions by storage team tags in the `group_writes_by_tag/2` function
- Encoded transactions have a binary format with version, size, key-value frames, and CRC32 checksum

## Discovery

### Current EncodedTransaction Module Capabilities

**File:** `/Users/j5n/Workspace/bedrock/lib/bedrock/data_plane/log/encoded_transaction.ex`

**Existing Functions:**
1. `encode/1` - Encodes a transaction to binary format
2. `decode/1` - Decodes binary back to transaction
3. `transform_by_removing_keys_outside_of_range/2` - Filters by key range
4. `transform_by_excluding_values/1` - Removes values, keeps only keys
5. `version/1` - Extracts version from encoded transaction
6. `key_count/1` - Counts keys in encoded transaction
7. `validate/1` - Validates CRC32 checksum

**Binary Structure:**
- 8-byte version (big endian)
- 4-byte size of key-value frames (big endian) 
- Key-value frames (sorted by key):
  - 4-byte frame size (big endian)
  - 2-byte key size (big endian)
  - key bytes
  - value bytes
- 4-byte CRC32 checksum (big endian)

### Current Commit Proxy Processing

**File:** `/Users/j5n/Workspace/bedrock/lib/bedrock/data_plane/commit_proxy/finalization.ex`

**Key Processing Functions:**
1. `group_writes_by_tag/2` - Groups transaction writes by storage team tags
2. `key_to_tag/2` - Maps individual keys to their storage team tag
3. `build_log_transactions/3` - Creates per-log transactions based on tag coverage
4. `push_transaction_to_logs/5` - Distributes transactions to logs

**Current Workflow:**
1. Transactions arrive as `{reads, writes}` tuples
2. Writes are grouped by storage team tag using `group_writes_by_tag/2`
3. Each tag gets its own `Transaction.t()` with combined writes
4. Transactions are encoded and sent to logs based on tag coverage

### Storage Team Configuration

**File:** `/Users/j5n/Workspace/bedrock/lib/bedrock/control_plane/config/storage_team_descriptor.ex`

**Storage Team Structure:**
```elixir
%{
  tag: Bedrock.range_tag(),
  key_range: Bedrock.key_range(), 
  storage_ids: [Storage.id()]
}
```

**Key Range Logic:**
- Keys belong to team if `key >= start_key && key < end_key`
- End key can be `:end` for the final range
- Uses lexicographic ordering

### Per-Log Splitting Current Implementation

**Location:** `finalization.ex` lines 666-688

The commit proxy currently:
1. Builds a map of `log_id -> [tags_covered]`
2. For each log, collects writes from all covered tags
3. Merges writes into a single transaction per log
4. Encodes and sends the combined transaction

## Open Questions

1. What specific slicing functionality is needed beyond the existing `transform_by_removing_keys_outside_of_range/2`?
2. Should slicing work at the individual key level or at the storage team tag level?
3. Are there performance requirements for the slicing operations?
4. Should sliced transactions maintain their original CRC32 checksums or recalculate?
5. How should empty slices be handled (when no keys match the slice criteria)?
6. Should there be batch slicing operations for multiple tags at once?
7. What error handling is needed when slicing fails?
8. Are there specific memory or CPU constraints for the slicing operations?
9. Should slicing preserve the original transaction version across all slices?
10. How should slicing interact with the existing key range filtering?

## Requires Clarification

1. Explain the specific use case where encoded transaction slicing by storage team tags is needed
2. Identify performance bottlenecks in the current per-log splitting implementation
3. Specify whether slicing should happen before or after conflict resolution
4. Clarify if slicing is meant to replace the current grouping mechanism or supplement it
5. Detail the expected input/output format for the slicing functions
6. Explain how sliced transactions would be distributed to logs differently than current approach
7. Identify any compatibility requirements with existing EncodedTransaction functions
8. Specify testing requirements for the new slicing functionality
9. Detail memory usage patterns and optimization requirements
10. Clarify integration points with the commit proxy finalization pipeline

## Current Analysis

The existing codebase has substantial functionality for manipulating encoded transactions:

**Strengths:**
- Efficient binary format with CRC32 integrity checking
- Key range filtering already implemented 
- Transaction grouping by storage team tags working in finalization
- Well-structured pipeline for processing transactions

**Potential Gaps:**
- No direct way to slice encoded transactions by storage team tags without decoding
- Current grouping happens after decoding, which may be inefficient
- Limited batch processing capabilities for multiple tag-based slices

The investigation suggests that while key range filtering exists, there may be a need for more efficient storage team tag-based slicing that can work directly on encoded transactions without full decode/re-encode cycles.