# Technical Specification: Move Transaction Encoding from Commit Proxy to Transaction Builder

Push transaction encoding upstream from commit proxy to transaction builder to enable easier enforcement of transaction size limits.

## Summary
- **Type:** refactoring
- **Complexity:** medium
- **Breaking Change:** yes
- **Confidence:** 85%

## Current Implementation

### Affected Components
| File Path | Functions | Current Behavior |
|-----------|-----------|------------------|
| lib/bedrock/cluster/gateway/transaction_builder/committing.ex | prepare_transaction_for_commit/3 | Returns {read_info, writes} with raw Elixir maps |
| lib/bedrock/data_plane/commit_proxy/finalization.ex | push_transaction_to_logs/5 | Encodes transaction using EncodedTransaction.encode/1 |
| lib/bedrock/data_plane/log/encoded_transaction.ex | encode/1 | Creates binary format with version, sorted key-value frames, CRC32 |

### Detailed Current Behavior

**Transaction Flow:**
1. Transaction Builder calls `prepare_transaction_for_commit/3` returning `{read_info, writes}` where:
   - `read_info` is either `nil` or `{read_version, [keys]}`
   - `writes` is `%{key => value}` (raw Elixir map)
2. This unencoded data is sent to Commit Proxy via `CommitProxy.commit/2`
3. Commit Proxy receives tuples, converts to `Transaction.t()` format: `{version, writes_map}`
4. During finalization at line 780, encodes using `EncodedTransaction.encode/1` before sending to logs

**Current Binary Format:**
- 8-byte version (big endian)
- 4-byte size of key-value frames (big endian)
- Key-value frames (sorted by key):
  - 4-byte frame size (big endian)
  - 2-byte key size (big endian)
  - Key bytes
  - Value bytes
- 4-byte CRC32 checksum (big endian)

**Per-Log Distribution:**
- Commit proxy creates different encoded transactions for each log based on storage team tag coverage
- Each log receives writes only for tags it covers
- Multiple logs may receive different subsets of the same transaction

## Required Changes

### Behavior Modifications

1. **Transaction Builder Encoding**: Transaction builder will encode complete transactions as binary format before sending to commit proxy
2. **Interface Change**: Modify `CommitProxy.commit/2` to accept binary encoded transactions instead of `{read_info, writes}` tuples
3. **Size Limit Enforcement**: Add size validation on encoded transactions with configurable limit (default 10MB)
4. **Exception Handling**: Reject oversized transactions with detailed error message including actual vs limit sizes

### Implementation Steps

#### Step 1: Add System Parameter for Transaction Size Limit
- **File:** System parameters configuration
- **Change:** Add `transaction_size_limit_bytes` parameter with default value of 10,485,760 (10MB)

#### Step 2: Modify Transaction Builder to Encode Transactions
- **File:** lib/bedrock/cluster/gateway/transaction_builder/committing.ex
- **Function:** prepare_transaction_for_commit/3
- **Changes:**
  1. Encode the transaction using `EncodedTransaction.encode/1`
  2. Check encoded size against configured limit
  3. Throw exception with size details if limit exceeded
  4. Return encoded binary instead of `{read_info, writes}`

#### Step 3: Update Transaction Builder Initialization
- **File:** Gateway transaction builder creation
- **Change:** Pass size limit from system parameters to transaction builder at creation time

#### Step 4: Modify Commit Proxy Interface
- **File:** lib/bedrock/data_plane/commit_proxy/finalization.ex
- **Function:** CommitProxy.commit/2
- **Changes:**
  1. Accept binary encoded transaction instead of tuple format
  2. Use `EncodedTransaction` utilities for per-log splitting
  3. Remove existing encoding call at line 780

#### Step 5: Update Per-Log Transaction Splitting
- **File:** lib/bedrock/data_plane/commit_proxy/finalization.ex
- **Function:** build_log_transactions/3
- **Changes:**
  1. Use `EncodedTransaction.transform_by_removing_keys_outside_of_range/2` for log-specific transactions
  2. Work with encoded binary format instead of raw maps
  3. Maintain existing tag-based distribution logic

#### Step 6: Add Size Validation Exception
- **Location:** Transaction builder size checking
- **Exception Type:** TransactionSizeExceededException
- **Message Format:** "Transaction size {actual_size} bytes exceeds limit of {limit_size} bytes"

## Edge Cases & Error Handling

| Scenario | Current Behavior | New Behavior |
|----------|------------------|--------------|
| Transaction exceeds size limit | N/A | Throw TransactionSizeExceededException with size details |
| Empty transaction | Encodes minimal structure | Validate against minimum size, allow if under limit |
| Nested transaction flattening | Size unknown until commit proxy | Size check on final flattened transaction |
| Per-log splitting with oversized transaction | Would fail at log level | Preventative rejection before reaching logs |
| Network timeout during commit | Unhandled exception | Same behavior, rejection happens earlier |

## Testing Requirements

### Unit Tests
- Test size limit enforcement with transactions at, below, and above limit
- Test exception message includes correct actual and limit sizes
- Test encoded transaction format matches existing encoding
- Test per-log splitting with encoded transactions
- Test empty transaction handling
- Test nested transaction size validation

### Integration Tests
- Test full transaction flow with encoding moved upstream
- Test commit proxy processing of encoded transactions
- Test system parameter configuration and propagation
- Test performance impact of upstream encoding
- Test backwards compatibility during migration

## Dependencies & Impacts

### Direct Dependencies
- `EncodedTransaction` module utilities for encoding and slicing
- System parameters configuration for size limit
- No new package installations required

### Downstream Impacts
- **Breaking Change**: Commit proxy interface changes from accepting tuples to binary
- Transaction size validation happens earlier in pipeline
- Potential performance impact from earlier encoding
- Maintains existing per-log distribution behavior
- No impact on transaction batching mechanisms

### Backwards Compatibility
- Requires coordinated deployment due to interface changes
- Consider feature flag for gradual rollout
- May need temporary dual interface support during migration

## Implementation Checklist

- [ ] Add transaction_size_limit_bytes system parameter
- [ ] Implement size checking in transaction builder with detailed exceptions
- [ ] Modify prepare_transaction_for_commit/3 to return encoded binary
- [ ] Update CommitProxy.commit/2 to accept binary transactions
- [ ] Modify build_log_transactions/3 to use EncodedTransaction utilities
- [ ] Remove existing encoding call in push_transaction_to_logs/5
- [ ] Add comprehensive unit tests for size validation
- [ ] Add integration tests for new transaction flow
- [ ] Performance benchmarking for encoding moved upstream
- [ ] Documentation updates for new transaction interface

## Key Decisions Made

1. **Transaction Builder Encoding**: Transaction builder will encode transactions as binary format instead of passing maps/tuples
2. **Storage Layout Separation**: Transaction builder does NOT need to know about shards/log layout - commit proxy will continue handling the splitting of encoded transactions across logs
3. **Size Limit Target**: Size limit enforcement should be on the total encoded transaction size (not raw data)
4. **Size Limit Default**: Default size limit of 10MB for encoded transactions
5. **Size Limit Configuration**: Size limit value provided to transaction builder at creation time, derived from system parameters by the gateway
6. **Size Limit Violation Handling**: Reject entire transaction with exception if size limit is exceeded
7. **Purpose**: This is a preventative measure to catch oversized transactions early
8. **Exception Details**: Exception should include size details (actual vs limit)
9. **Interface Changes**: Change existing interface to accept binary instead of {read_info, writes}
10. **Slicing Utilities**: Use existing EncodedTransaction utilities for slicing (transform_by_removing_keys_outside_of_range, etc.)
11. **Size Limit Scope**: Size limit applies to entire encoded transaction including metadata
12. **System Parameter**: New system parameter for size limit configuration
13. **Size Check Timing**: Size check happens just before pushing to commit proxy (both transaction builder and commit proxy can enforce)