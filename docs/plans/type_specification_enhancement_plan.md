# Type Specification Enhancement Plan

## Procecdure

1. Identify all files to be processed, save the root-relative paths to a
file (type_spec_files_to_process.txt). If the file is already present, just use 
it.

2. Methodically process each file in the list, one at a time, applying the rules
to every spec in the file.

3. Remove the file from the list.

4. Repeat from step 2.

## Rules

1. Replace Generic Container Types with Specific Content Types

- map() → %{Worker.id() => Worker.ref()}
- keyword() → [timeout: pos_integer(), retry: boolean()]
- list() → [Transaction.t()]

2. Replace Vague Types with Domain-Specific Types

- term() → Bedrock.version() | :unavailable
- any() → String.t() | :log | atom() | pid()
- binary() → Bedrock.key() or Bedrock.value()

3. Replace Generic Error Types with Specific Error Unions

- {:error, term()} → {:error, :timeout | :unavailable | :not_found}
- {:error, atom()} → {:error, File.posix()}
- {:error, any()} → {:error, :worker_not_found | {:failed_to_start, reason}}

4. Enhance Child Spec Options from Generic to Specific

- child_spec(Keyword.t()) → child_spec([cluster: module(), path: Path.t()])
- opts :: keyword() → opts :: [lock_token: binary(), epoch: Bedrock.epoch()]

5. Replace Overloaded Specs with Multiple Specific Specs

- One spec with term() | %{term() => term()} → Two separate specs for single vs multiple cases
- info(atom() | [atom()]) → @spec info(atom()) :: + @spec info([atom()]) ::

6. Use Existing @type Declarations Instead of Inline Types

- %{tag: binary(), storage_ids: [binary()]} → StorageTeamDescriptor.t()
- {pid(), non_neg_integer()} → {Director.ref(), Bedrock.epoch()}

7. Create Domain-Specific Types for Complex Structures

- map() for waiting lists → @type waiting_list :: %{transaction_id() => ack_fn()}
- keyword() for parameters → @type parameter_changes :: [parameter_change()]

8. Specify Exact Return Values Instead of Generic Types

- :: term() → :: :ok | {:error, :invalid_parameters}
- :: any() → :: Bedrock.version() | nil

9. Use Module-Qualified Types for Cross-Module References

- GenServer.server() → Bedrock.ControlPlane.Coordinator.ref()
- pid() in context → Log.ref() or Storage.ref()

10. Consolidate Duplicate Specs into Single Precise Declarations

- Multiple similar specs → One comprehensive spec with proper parameter naming
- Remove redundant specs that specify the same function signature