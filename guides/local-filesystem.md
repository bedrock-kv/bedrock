# LocalFilesystem build and deployment

LocalFilesystem stores complete objects as ordinary files. It is intended for
local development and testing. Version-matched writes use a small POSIX NIF;
transaction algorithms, recovery and supervision remain Elixir/OTP. The package
is therefore not pure BEAM and does not require a native storage engine.

All four mutations (put, create-only, conditional replacement and delete) acquire
the same permanent `.bedrock-lock` inode in the object's physical parent
directory. Comparison and publication occur in one native operation. Different
keys in one directory contend deliberately; directory symlink and filesystem
case/normalization aliases share the same lock. Metadata is hidden from listing
and cannot be replaced through mutation keys; absolute/traversal paths and
object symlinks are rejected. Reserved `.bedrock-lock` path components are
case-insensitive. Do not externally replace parent directories, lock files, or
object symlink/hard-link aliases while using the backend.

Quiesce and upgrade every writer sharing a root together. Old versions do not
honor these locks. Only local Linux/macOS filesystems with coherent flock and
atomic same-directory rename/link/unlink are covered. NFS, SMB, FUSE and arbitrary
multi-host mounts require separate verification. Lock files persist forever as
metadata; their existence does not indicate a held lock, and deleting them is
not recovery. The OS releases ownership when descriptors close or a VM dies.

Killing an Erlang caller is not cancellation: an executing dirty NIF may remain
blocked or publish after the caller's DOWN notification. Treat the operation's
outcome as unknown. An acquired lock remains with the native operation until it
finishes. Whole-VM termination closes descriptors. Blocked native filesystem
calls consume dirty-I/O schedulers, and there is no timeout-based stealing or
bounded liveness guarantee on stalled filesystems.

Content is written to an exclusive same-directory scratch, synced, then published
whole. Ordinary failures remove scratch; killed writers can leave hidden scratch
orphans (bedrock-ck3). Parent-directory fsync is deliberately not added: power
loss can still lose a just-published directory entry. Tokens are SHA256 content
identity, so no-op writes and A-to-B-to-A changes retain/reuse tokens.

## Source builds and packages

Linux and macOS builds require a C compiler, make and OTP development headers,
including for applications using only S3. Install build-essential and Erlang
headers on Linux or Xcode Command Line Tools on macOS. Windows and other targets
fail at build time; there is no unlocked fallback. Supported BEAM toolchains
follow the project's OTP27–29 / Elixir >=1.17 matrix.

The Hex package contains C source and Makefile, not a developer-machine binary.
elixir_make builds `priv/local_filesystem_mutation.so` in the application's build
path. Loading uses `:code.priv_dir(:bedrock)`, including in releases, never cwd.
Missing/incompatible artifacts make local mutation unavailable instead of
silently selecting unsafe behavior. S3 code has no runtime dependency on the
local NIF, but the source package still requires its build toolchain.

`mix clean` removes generated native artifacts and `mix compile` rebuilds them.
The Makefile honors CC, CFLAGS, LDFLAGS, ERTS_INCLUDE_DIR, MIX_APP_PATH and
TARGET_OS (Linux or Darwin). Cross builds must provide a matching target compiler,
sysroot and OTP headers and explicit TARGET_OS; do not execute target binaries
on the build host. Keep output/cache paths separate by target OS, architecture
and OTP. No precompiled-binary distribution or supply-chain guarantee is implied.
