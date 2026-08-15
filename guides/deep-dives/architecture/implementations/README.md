
# Implementation Components

These are concrete storage engines that implement the data plane interfaces:

- **[Olivine](olivine.md)** - The materializer engine: a versioned page index fed by a per-shard stream from each log's Demux
- **[Shale](shale.md)** - An example disk-based write-ahead-log implementation with segment file management

Implementation components can be swapped or configured based on performance requirements, hardware characteristics, and deployment constraints.
