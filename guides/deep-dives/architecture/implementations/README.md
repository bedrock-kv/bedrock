
# Implementation Components

These are concrete storage engines that implement the data plane interfaces:

- **Olivine** - The multi-version materializer engine (versioned index over split data/index files, streaming its shard from the log's Demux)
- **[Shale](shale.md)** - An example disk-based write-ahead-log implementation with segment file management

Implementation components can be swapped or configured based on performance requirements, hardware characteristics, and deployment constraints.
