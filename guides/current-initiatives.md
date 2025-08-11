# Current Initiatives

## Integer to Binary conversion of versions within the system

We recently began reworking the way versions are handled within the system. they used to be 64-bit integers, but now we're encoding them as 8-byte binaries (unsigned big-endian integer encoding). Lexicographically, the new versions should sort in the same way that the integers did, so all of the max/min/sort/etc. operations should continue to work.
