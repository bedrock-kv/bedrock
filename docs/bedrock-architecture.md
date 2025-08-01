<!-- livebook:{"file_entries":[{"name":"bedrock-architecture-2.jpg","type":"attachment"}]} -->

# Bedrock: An embedded, distributed key-value store with guarantees beyond ACID

## Why does it exist?

Bedrock is a distributed key-value store built in Elixir, designed to be embedded within other applications while offering ACID properties and high performance. The development of Bedrock was motivated by several persistent problems faced by developers when using traditional SQL-based databases and scaling them with the growth of their applications. It aims to address many of these issues without sacrificing the strong guarantees that SQL-based systems provide over traditional NoSQL systems.

#### The Constraints of SQL-based Databases

Traditional SQL databases require developers to structure their business logic and rules according to the constraints of tables, rows, and relational schemas. While SQL databases provide critical transactional guarantees and a centralized storage system that ensures consistent data visibility across multiple application nodes, this comes at a significant cost. Developers must funnel their data into a specific structure that SQL demands, resulting in code that often obscures the true logic of the problem being solved and requires extensive boilerplate for basic operations. This constraint leads to inefficiencies and makes it difficult to maintain and extend application logic in a natural way. These systems provide _a lot_ of value, but that value comes with _a lot_ of strings attached as to how the work gets done.

#### Flexibility for Development and Scaling

Commonly available SQL databases are often not built to scale _easily_ as the needs of an application evolve. What works at at a prototype-scale will often differ from what is required as the application grows. Physical machines _can_ usually be upgraded, but not without incurring downtime. Cloud-based deployments allow developers to opt for ever-larger machine-instances to increase capacity, but the cost of such scaling does not grow linearly. Machines with double the memory or CPU are typically more than twice as expensive. It's not usually possible to scale the database up and down the same way that modern applications can be, where instances are added and removed depending on the current load on the application.

Additionally, developers face significant challenges when attempting to scale the read-capicity of these storage systems. Though it's possible to setup one or more "read replicas" to increase query capacity, applications must be aware of these replicas and manage traffic to them accordingly, while ensuring that writes still go to the primary instance. Depending on the application, this approach can add a lot of complexity because replication isn't instant and so data read from a replica will not always match the data on the primary. This creates potential synchronization issues that must be managed by the application developer.

#### Underutilized Local Storage

Applications often run on machines equipped with local, fast storage that remains underutilized due to the inherent difficulty of building distributed systems capable of leveraging this resource. In order to use these resources, developers would have to create distributed systems from scratch for every application. This is unreasonable and impractical. This model forces the application to communicate with the network for nearly all of the work that it must do and assume all of the latencies that that introduces. If the data necessary to satisfy a read were readily available on the application's own machine, then no network traffic would be required to satisfy that request. This is the general idea behind various caching schemes, but those often run into complicated consistency issues when moving the application beyond a single node.

## What problems does it solve?

Bedrock addresses key challenges by providing a distributed key-value store that is:

* ACID-compliant with _strict_ serialization and repeatable reads, ensuring data consistency and reliability.
* Fast and embeddable, simplifying integration within existing applications.
* Optimized for local storage use, boosting performance by reducing the need for external database and network calls.
* Scalable, from in-memory instances for development and testing to large-scale production deployments with distributed, replication storage.

#### Key Features

Bedrock presents itself to the developer as a single, continuous, sorted key-value space—essentially a large, sorted "Map." This structure allows keys to be accessed in sorted order and values to be manipulated in a transactional manner. While it is possible to store keys and values as raw binaries, Bedrock also supports built-in types like tuples, lists, and maps, offering developers more flexibility in data representation. Keys composed of Elixir types (e.g., tuples, integers) are stored in the same order as if placed in a list and processed by `Enum.sort/1`, simplifying range queries and indexing.

#### Flexibility and Scalability

Bedrock is designed with flexibility at its core, capable of operating as a lightweight in-memory instance or scaling seamlessly to a fully distributed system across multiple nodes. For developers, this means Bedrock can be used for running integration tests on a development workstation or prototyping on a single node without complex configurations. As applications grow and demand greater durability and fault tolerance, Bedrock scales to support replicated storage across nodes, enhancing both durability and performance. This distributed capability not only ensures data persistence but also scales read and write capacity as additional nodes are added, adapting to the evolving needs of the application.

#### Performance and Cost Efficiency

Bedrock can be configured to use local storage close to the application, maintaining a consistent view of data across all nodes and ensuring data durability even in the event of node failure. This configuration enhances performance and can eliminate the significant costs associated with centralized, managed SQL databases. By offering an embedded, inherently distributed solution, Bedrock enables applications to scale up while preserving data consistency and reliability. Whether used as a fast distributed cache, primary storage, or a hybrid solution, Bedrock provides the flexibility to meet diverse application requirements.

## How does it work?

Bedrock’s architecture is designed around simple, specialized components, each with a clear and straightforward role. Each node will fill one or more of the roles, and a single-node system will perform all of them:

* **Coordinators**: Use a consensus protocol to durably store the system's configuration and establish a Director.

* **Director**: Gets the system back to operational after a cold start or a failure. It monitors the running system to ensure that it flows smoothly.

* **Sequencer**: Ensures a global-order to transactions by assigning version numbers.

* **Read-Version Proxies**: Get transactions started by providing read-versions and gathering statistics to help efficiently manage MVCC.

* **Commit Proxies**: Batch transactions and streamline the process of getting them resolved and sent to the logs.

* **Resolvers**: Apply Multi-Version Concurrency Control (MVCC) to detect and resolve conflicts in transactions, maintaining data integrity.

* **Logs**: Accept completed transactions and get them to disk as quickly as possible to ensure durability.

* **Storage Processes**: Serve data to clients and follow the logs to get the latest transactions.

These components collaborate to establish a global order of operations, ensuring that Bedrock maintains its ACID properties for data consistency and reliability. Individual components can fail, and the system is designed to detect and recover from these failures while doing everything possible to continue processing transactions.

<!-- livebook:{"break_markdown":true} -->

![](files/bedrock-architecture-2.jpg)

## Glossary

#### Commit Proxy

A service that receives transactions, sifts out any conflicts and gets the rest to durable storage as quickly as possible. It ensures that the necessary steps for resolution and logging are followed and helps maintain the guarantee of consistency and strict transaction serialization. It's workflow involves the following steps:

1. Transaction Receipt and Batching

* *Receiving Transactions*: The service receives an incoming transaction and places the caller on a waiting list. If there is no active batch, then a new one is started. Otherwise, the transaction is added to the batch in the order in which it was received.
* *Batch Closure*: The batch will remain open for a brief period (a few milliseconds) _or_ until a predefined number of transactions is reached.

1. Conflict Resolution

* *Commit Version Retrieval*: Once a batch is closed, the service retrieves the next commit version from the `Sequencer`.
* *Determining Resolvers*: The keys within each transaction are compared to the key ranges managed by each `Resolver` to identify which resolvers need to be involved in settling the transaction.
* *Submitting to Resolvers*: The service submits the batch to each of the relevant Resolvers.
* *Resolution Outcome*: After resolution, the service identifies which transactions will be aborted due to conflicts and which transactions can proceed.
* *Notifying Callers*: Callers whose transactions are aborted are removed from the waiting list and notified. They are now free to try again.

1. Commit Preparation and Logging

* *Compaction for Commit*: The remaining transactions do not conflict and can be compacted into a single operation.
* *Sending to Log Servers*: The service identifies the `Log` servers responsible for the relevant key ranges and sends the commit version along with the compacted transaction to each of them.
* *Acknowledgement and Notification*: The service waits for acknowledgements from all `Log` servers. Once received, it removes the remaining callers from the waiting list and informs them of that their commit was successful and the commit version.

<!-- livebook:{"break_markdown":true} -->

#### Last Committed Version (LCV)

The version of the last committed transaction. This number advances as transactions are added to the system.

<!-- livebook:{"break_markdown":true} -->

#### Minimum Read Version (MRV)

The oldest version that can still be reliably accessed across any storage node or used for conflict resolution. It moves forward over time, following the last committed transaction (LCV) to form a sliding window of information necessary for checking conflicts on transactions that are in-flight. By advancing the MRV as new transactions are processed, the system maintains efficient memory usage while ensuring transactions can be validated and versioned-reads can be processed reliably.

* Transaction Initiation: When a transaction begins a read, it must first obtain a read version from the Sequencer and notify its local Gateway, which provides the transaction with a lease for that read version.
* Lease Renewal: If the transaction wants to exceed the duration of the lease, it must renew with the Gateway; otherwise, the lease expires, and the system might discard essential bookkeeping data, leading to potential rejection of the transaction.
* Gateway Aggregation: The gateway reviews and removes expired leases, identifying the smallest read version with an active lease to establish its minimum read version. This value is reported to the Director.
* Director Aggregation: The Director gathers the minimum read versions from all Gateway instances and identifies the smallest value, determining the global MRV. The Director then forwards this value to the rest of the system.
