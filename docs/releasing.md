# Releasing Cellule

Cellule ships seven crates as one matched release. The canonical package
version is `[workspace.package].version` in the root `Cargo.toml`; each crate
inherits it. Internal dependencies use Cargo's SemVer-compatible ranges at
the current compatibility floor. The release script checks this set and
publishes it in dependency order.

## Version policy

Use SemVer versions and tags with the same value, prefixed by `v`:

| Change | Before `1.0.0` | From `1.0.0` |
| --- | --- | --- |
| Breaking API or persisted/wire contract change | Increment minor; reset patch to zero. | Increment major; reset minor and patch to zero. |
| Backwards-compatible feature | Increment minor; reset patch to zero. | Increment minor; reset patch to zero. |
| Backwards-compatible fix or documentation-only release | Increment patch. | Increment patch. |

Use prereleases such as `0.2.0-rc.1` when needed. Do not add SemVer build
metadata to a crates.io release version. Keep all seven package versions
matched. For stable releases, set internal dependency ranges to `0.MINOR.0`
while the major version is zero and to `MAJOR.0.0` after `1.0.0`; Cargo's
default caret requirement admits compatible patches or minors. During a
prerelease, pin internal dependencies to that exact prerelease version so the
candidate crates can resolve one another.

## Contract review

Before changing a persisted or wire contract, review its writers, readers,
tests, and migration path:

| Contract | Release review |
| --- | --- |
| Cell and module identity | Namespace, partition rule, role, schema version, operation IDs, and descriptor digest. |
| Durable state | SQLite migrations, LTX encoding, roots and object paths, WAL boundary, and recovery verification. |
| Coordination | Owner fence, authority compare-and-swap, request outcome, receipt, and follower proof. |
| Peer protocol | Signed messages, replay windows, and compatible rollout behavior. |
| Blob lifecycle | Cross-Cell references, quiescence, and deletion grace boundary. |

The [API guide](api.md) describes author-visible contracts. The
[architecture guide](architecture.md) explains publication and recovery.
Provider, scale, compatibility, and fault claims need their own measured
artifacts; use the [qualification profiles](../crates/cellule-runtime/qualification/README.md)
and [delivery evidence guide](../crates/cellule-runtime/docs/delivery.md).

## Release cycle

1. **Prepare a release PR.** Update `[workspace.package].version` and adjust
   the internal dependency range when moving to a new compatibility line.
   Add a dated,
   non-empty section for that version in [`CHANGELOG.md`](../CHANGELOG.md),
   including contract changes and migration notes. Regenerate `Cargo.lock`
   with an unlocked workspace check, then use the repository's locked CI
   commands. `python3 scripts/release.py check` validates the version,
   changelog, and seven-crate set.
2. **Merge the green PR.** The release commit must be reachable from `main`.
   The Rust CI workflow also packages the complete workspace.
3. **Tag that exact commit.** For example:

   ```sh
   git tag -a v0.1.0 -m "Cellule v0.1.0"
   git push origin v0.1.0
   ```

   The tag starts `.github/workflows/release.yml`. It verifies the tag against
   the workspace version and main branch, reruns the full Rust CI workflow,
   then publishes the seven packages to crates.io. It waits for each version
   to appear in both the crates.io API and sparse index before publishing its
   dependents. The workflow creates a GitHub Release from the matching
   changelog section after all crates are visible.
4. **Confirm the release.** Check the workflow, GitHub Release, and all seven
   crates.io version pages. Smoke-test the published set from a clean consumer
   when making an author-facing or compatibility-sensitive release. Link any
   relevant qualification evidence in the GitHub Release notes.

If a workflow retry finds a crate version already published, it compares the
published checksum with the tag's package archive and skips only an exact
match. A checksum mismatch stops the release for investigation. A failed
partial release can be retried with the same tag after the cause is fixed;
published versions cannot be replaced.

## crates.io publisher setup

The designated crates.io publisher is the [`forhappy` account](https://crates.io/users/forhappy).
The account must own all seven crate names. Credentials do not belong in the
repository or GitHub secrets.

Crates.io requires a crate's first version to be published manually before
trusted publishing can be configured. For the initial `0.1.0` release, after
the versioned commit has merged and passed CI:

1. On a trusted machine logged in to crates.io as `forhappy`, package and
   publish each crate in dependency order with the same toolchain:

   ```sh
   cargo +1.97.0 publish -p cellule-types --locked
   cargo +1.97.0 publish -p cellule-store --locked
   cargo +1.97.0 publish -p cellule-ltx --locked
   cargo +1.97.0 publish -p cellule-runtime --locked
   cargo +1.97.0 publish -p cellule-app --locked
   cargo +1.97.0 publish -p cellule-host --locked
   cargo +1.97.0 publish -p cellule-peer-http --locked
   ```

   Allow each package version to reach the registry index before publishing
   its dependents. Keep the account token in Cargo's local credentials file;
   never paste it into the repository or workflow configuration.
   The first release has a dev-only cycle: `cellule-app` tests depend on
   `cellule-host`, while `cellule-host` depends on `cellule-app`. For the
   one-time `0.1.0` bootstrap, publish `cellule-app` with that dev-dependency
   temporarily removed and `--no-verify`, restore the manifest immediately,
   then publish `cellule-host`. Subsequent releases use the normal workflow
   after both crate names exist on crates.io.
2. In the settings for **each** crate, add a GitHub Actions trusted publisher
   with repository `crabbuild/cellule`, workflow file `release.yml`, and
   environment `crates-io`. The workflow uses
   [crates.io trusted publishing](https://crates.io/docs/trusted-publishing)
   to obtain short-lived OIDC credentials; it stores no registry token.
3. Configure the GitHub `crates-io` environment to allow release tags and, if
   desired, require maintainer approval. Push the matching `v0.1.0` tag. The
   workflow verifies the manually published checksums, skips those uploads,
   and creates the GitHub Release. Later tags publish through OIDC.

The initial manual upload and trusted-publisher configuration are one-time
account setup. Do not change the release workflow filename or environment
without updating every crate's trusted-publisher configuration.
