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
3. persistent `coordinator.path`
4. persistent log path
5. persistent materializer path

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
