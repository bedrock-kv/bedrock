# Durability Profile Contract

`Bedrock.Durability` provides a machine-checkable contract for evaluating and
enforcing durability profile requirements.

## API

- `Bedrock.Durability.profile/1`
- `Bedrock.Durability.require/2`

## Checks

The profile currently evaluates:

1. `desired_replication_factor >= 3`
2. `desired_logs >= 3`
3. coordinator persistence enabled (`coordinator.persistent == true`, or
   implied by a coordinator path when unset)
4. persistent `coordinator.path`
5. persistent log path
6. persistent materializer path

## Result Shape

`profile/1` returns a `%Bedrock.Durability.Profile{}`:

- `status` - `:ok | :failed`
- `checks` - map of check name to `%{status, expected, actual, reason}`
- `reasons` - list of failure reason atoms
- `evaluated_at_ms` - evaluation timestamp

## Enforcement

`require/2` supports two modes:

- `:strict` - returns `{:error, {:durability_profile_failed, reasons}}` when
  profile fails.
- `:relaxed` - returns `:ok` even when profile fails.

This allows downstream adapters to fail fast in strict deployments while still
supporting gradual rollout in relaxed mode.

## Runtime Startup Enforcement

`Bedrock.Internal.ClusterSupervisor` enforces this profile at startup:

- `:relaxed` mode (default): logs warning and continues.
- `:strict` mode: raises and fails startup when checks fail.

Mode configuration is additive:

- top-level `:durability_mode` (`:strict | :relaxed`)
- or `durability: [mode: :strict | :relaxed]`

Desired sizing parameters can be provided via:

- coordinator cluster config (`parameters.desired_replication_factor`,
  `parameters.desired_logs`)
- or node config fallback (`durability: [...]`).

## Telemetry Events

Profile evaluations emit one of:

- `[:bedrock, :durability, :profile, :ok]`
- `[:bedrock, :durability, :profile, :failed]`

Measurements:

- `failed_checks` - number of failed checks

Metadata:

- `status` - `:ok | :failed`
- `reasons` - list of failure reason atoms
- `target_type` - input type (`:cluster_module | :node_config | :profile_input`)
- `target_module` - cluster module when applicable
