# Transaction Binary Format Specification v1.0

## Overview

The Transaction binary format is Bedrock's tagged binary encoding for transaction data that enables efficient distributed processing, robust validation, and selective component access. This format replaces simple map structures with self-describing binary sections, allowing components to extract only the data they need without deserializing entire transactions.

**Primary use cases:**

- High-performance transaction batching and processing
- MVCC conflict detection with minimal data extraction
- Durable transaction storage with integrity validation
- Distributed component coordination with reduced network overhead

## File Structure

```
┌─────────────────────────────────────────────────────────────┐
│                     Transaction Binary                      │
├─────────────────────────────────────────────────────────────┤
│ Header (8 bytes)                                            │
│ ┌─────────────┬─────┬─────┬─────────────┐                   │
│ │Magic "BRDT" │ Ver │Flags│Section Count│                   │
│ │   4 bytes   │ 1B  │ 1B  │   2 bytes   │                   │
│ └─────────────┴─────┴─────┴─────────────┘                   │
├─────────────────────────────────────────────────────────────┤
│ MUTATIONS Section (0x01) - Required                         │
│ ┌─────┬─────────┬──────────┬─────────────────────────────┐  │
│ │ Tag │  Size   │  CRC32   │         Payload             │  │
│ │ 1B  │ 3 bytes │ 4 bytes  │       Variable              │  │
│ │0x01 │         │          │ {:set, key, value} ...      │  │
│ └─────┴─────────┴──────────┴─────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│ READ_CONFLICTS Section (0x02) - Optional                    │
│ ┌─────┬─────────┬──────────┬─────────────────────────────┐  │
│ │ Tag │  Size   │  CRC32   │         Payload             │  │
│ │ 1B  │ 3 bytes │ 4 bytes  │       Variable              │  │
│ │0x02 │         │          │ {version, [ranges]}         │  │
│ └─────┴─────────┴──────────┴─────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│ WRITE_CONFLICTS Section (0x03) - Optional                   │
│ ┌─────┬─────────┬──────────┬─────────────────────────────┐  │
│ │ Tag │  Size   │  CRC32   │         Payload             │  │
│ │ 1B  │ 3 bytes │ 4 bytes  │       Variable              │  │
│ │0x03 │         │          │ [conflict_ranges]           │  │
│ └─────┴─────────┴──────────┴─────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│ COMMIT_VERSION Section (0x04) - Added by Commit Proxy       │
│ ┌─────┬─────────┬──────────┬─────────────────────────────┐  │
│ │ Tag │  Size   │  CRC32   │         Payload             │  │
│ │ 1B  │ 3 bytes │ 4 bytes  │       Variable              │  │
│ │0x04 │         │          │ commit_version              │  │
│ └─────┴─────────┴──────────┴─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

Section Access Patterns:
• Commit Proxy → Resolver: READ_CONFLICTS + WRITE_CONFLICTS only
• Commit Proxy → Logs: All sections for persistence  
• Logs → Storage: MUTATIONS + COMMIT_VERSION for application
```

## Header Specification

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 4 bytes | Magic Number | `0x42524454` ("BRDT" in ASCII) |
| 0x04 | 1 byte | Format Version | Current version: `0x01` |
| 0x05 | 1 byte | Flags | Reserved for future use (must be `0x00`) |
| 0x06 | 2 bytes | Section Count | Number of sections following header (big-endian) |

**Total header size: 8 bytes**

## Section Types

### Type 0x01: MUTATIONS (Required)

**Purpose:** Contains all transaction operations that modify data.

**Structure:**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 1 byte | Tag | `0x01` |
| 0x01 | 3 bytes | Payload Size | Size of payload in bytes (big-endian) |
| 0x04 | 4 bytes | CRC32 | Checksum of tag + size + payload |
| 0x08 | Variable | Payload | Encoded mutation operations |

**Payload format:** List of operations using opcode-based encoding with automatic size optimization.

#### Mutation Encoding

Each mutation uses an 8-bit opcode with 5-bit operation + 3-bit size variant:

```
Opcode Structure:
┌─────────────┬─────────────┐
│   Operation │   Variant   │
│   (5 bits)  │   (3 bits)  │
│   bits 7-3  │   bits 2-0  │
└─────────────┴─────────────┘
```

**Operation Types:**

| Operation | Base Value | Description |
|-----------|------------|-------------|
| SET | 0x00 | Set key to value |
| CLEAR | 0x01 | Clear key or key range |

**SET Operation Variants:**

| Variant | Opcode | Format | Use Case |
|---------|--------|--------|----------|
| 0 | 0x00 | 16-bit key len + 32-bit value len | Large keys/values |
| 1 | 0x01 | 8-bit key len + 16-bit value len | Medium optimization |
| 2 | 0x02 | 8-bit key len + 8-bit value len | Most compact |

**CLEAR Operation Variants:**

| Variant | Opcode | Format | Use Case |
|---------|--------|--------|----------|
| 0 | 0x08 | Single key, 16-bit length | Large keys |
| 1 | 0x09 | Single key, 8-bit length | Small keys |
| 2 | 0x0A | Key range, 16-bit lengths | Large range keys |
| 3 | 0x0B | Key range, 8-bit lengths | Small range keys |

**Binary Layout Examples:**

```
SET "user123" = "active" (small key/value):
0x02 0x07 "user123" 0x06 "active"
 ↑    ↑       ↑       ↑      ↑
 │    │       │       │      └─ Value: "active"
 │    │       │       └─ Value length: 6 bytes
 │    │       └─ Key: "user123"  
 │    └─ Key length: 7 bytes
 └─ Opcode: SET_8_8 (variant 2)

CLEAR_RANGE "user000" to "user999":
0x0B 0x07 "user000" 0x07 "user999" 
 ↑    ↑       ↑       ↑       ↑
 │    │       │       │       └─ End key: "user999"
 │    │       │       └─ End key length: 7 bytes
 │    │       └─ Start key: "user000"
 │    └─ Start key length: 7 bytes
 └─ Opcode: CLEAR_RANGE_8 (variant 3)
```

**Automatic Variant Selection:**
- Encoder automatically selects most compact variant based on actual data sizes
- Maximizes space efficiency while maintaining fast parsing performance

### Type 0x02: READ_CONFLICTS (Optional)

**Purpose:** Contains read conflict ranges and read version for MVCC conflict detection.

**Structure:**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 1 byte | Tag | `0x02` |
| 0x01 | 3 bytes | Payload Size | Size of payload in bytes (big-endian) |
| 0x04 | 4 bytes | CRC32 | Checksum of tag + size + payload |
| 0x08 | Variable | Payload | `{read_version, [conflict_ranges]}` |

**When present:** Transaction performed reads that need conflict detection.

### Type 0x03: WRITE_CONFLICTS (Optional)

**Purpose:** Contains write conflict ranges for MVCC conflict detection.

**Structure:**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 1 byte | Tag | `0x03` |
| 0x01 | 3 bytes | Payload Size | Size of payload in bytes (big-endian) |
| 0x04 | 4 bytes | CRC32 | Checksum of tag + size + payload |
| 0x08 | Variable | Payload | List of write conflict ranges |

**When present:** Transaction has mutations that could conflict with other transactions.

### Type 0x04: COMMIT_VERSION (Added by Commit Proxy)

**Purpose:** Contains the version assigned by the Commit Proxy for transaction ordering.

**Structure:**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 1 byte | Tag | `0x04` |
| 0x01 | 3 bytes | Payload Size | Size of payload in bytes (big-endian) |
| 0x04 | 4 bytes | CRC32 | Checksum of tag + size + payload |
| 0x08 | Variable | Payload | Encoded version number |

**When added:** After successful conflict resolution and version assignment.

## Examples

### Minimal Valid Transaction (Write-Only)

```
42 52 44 54 01 00 00 01    # Header: Magic "BRDT", v1, flags=0, 1 section
01 00 00 0F 12 34 56 78    # MUTATIONS: tag=0x01, size=15, CRC32=0x12345678
83 F6 7B 3A 73 65 74 2C    # Payload: encoded {:set, "key", "value"}
6B 65 79 2C 76 61 6C 75
65 7D
```

**Breakdown:**

- Header identifies valid transaction with 1 section
- Single MUTATIONS section with set operation
- No conflict sections (write-only transaction)

### Complex Transaction with Conflicts

```
42 52 44 54 01 00 00 03    # Header: Magic "BRDT", v1, flags=0, 3 sections
01 00 00 0F AB CD EF 12    # MUTATIONS: tag=0x01, size=15, CRC32
[... mutation payload ...]
02 00 00 18 34 56 78 9A    # READ_CONFLICTS: tag=0x02, size=24, CRC32  
[... read conflicts payload ...]
03 00 00 0C 56 78 9A BC    # WRITE_CONFLICTS: tag=0x03, size=12, CRC32
[... write conflicts payload ...]
```

**Breakdown:**

- Transaction with reads and writes
- Includes all conflict sections for MVCC processing
- Will receive COMMIT_VERSION section after processing

## Component Access Patterns

Different system components extract specific sections for optimal performance:

**Transaction Builder → Commit Proxy:**

- Sends: MUTATIONS + READ_CONFLICTS + WRITE_CONFLICTS
- Purpose: Complete transaction data for validation and batching

**Commit Proxy → Resolver:**

- Extracts: READ_CONFLICTS + WRITE_CONFLICTS only
- Purpose: MVCC conflict detection without mutation data

**Commit Proxy → Logs:**

- Sends: All sections including COMMIT_VERSION
- Purpose: Durable persistence of complete transaction

**Logs → Storage:**

- Extracts: MUTATIONS + COMMIT_VERSION
- Purpose: Apply operations to versioned key-value store

## Parsing Algorithm

```
1. Read 8-byte header
2. Validate magic number (0x42524454)
3. Check format version (0x01)
4. Read section count from header
5. For each section:
   a. Read 1-byte tag
   b. Read 3-byte size (big-endian)
   c. Read 4-byte CRC32
   d. Read payload of specified size
   e. Validate CRC32 over tag + size + payload
   f. Process section based on tag type
6. Verify all required sections present (MUTATIONS)
```

## Version History

- **v1.0**: Initial specification with four section types
  - MUTATIONS (0x01): Transaction operations
  - READ_CONFLICTS (0x02): Read conflict ranges  
  - WRITE_CONFLICTS (0x03): Write conflict ranges
  - COMMIT_VERSION (0x04): Assigned transaction version

---

> **Implementation Details**: For encoding/decoding implementation, opcode variants, and streaming support, see the [Transaction Processing Deep Dive](../deep-dives/transactions.md#transaction-format).

## See Also

- [Transactions Overview](transactions.md) - Complete transaction processing and ACID guarantees
- [Commit Proxy](../deep-dives/architecture/data-plane/commit-proxy.md) - Transaction batching and format validation
- [Resolver](../deep-dives/architecture/data-plane/resolver.md) - MVCC conflict detection using transaction sections
