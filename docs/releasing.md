# Releasing

Releases are built and published by GitHub Actions. Do not upload packages from
a developer machine.

The version comes from `Cargo.toml`. Its release tag uses a `v` prefix, so
version `0.1.0` uses tag `v0.1.0`.

## One-time setup

The repository needs two GitHub environments:

- `testpypi`, limited to tags matching `v*`;
- `pypi`, limited to tags matching `v*` and protected by a required reviewer.

Configure a Trusted Publisher on TestPyPI and PyPI for owner `p-r-a-v-i-n`,
repository `django-native-postgres`, workflow `release.yml`, and the matching
GitHub environment. No PyPI token is required.

## Prepare

Use a release pull request and confirm:

1. `Cargo.toml` and the root package in `Cargo.lock` contain the release
   version.
2. The changelog heading contains the version and release date.
3. The Django version and fork revision agree in `pyproject.toml`, `uv.lock`,
   `README.md`, and `tests/wheel_smoke.py`.
4. The checks in `docs/development.md` pass.
5. All pull-request CI jobs pass.

The initial release is version `0.1.0` and remains experimental and pre-alpha.

## Test the package files

After merging the release pull request, manually run the **Distributions**
workflow on `main`.

It builds the platform wheels and source distribution without publishing them.
Confirm that every build and smoke test passes and that the workflow contains:

- Linux x86-64 and ARM64 wheels;
- macOS Intel and Apple Silicon wheels;
- a Windows x86-64 wheel; and
- one `.tar.gz` source distribution.

Do not create the release tag if this workflow fails.

## Publish

Create an annotated tag from the reviewed `main` commit:

```console
git switch main
git pull --ff-only
git tag -a v0.1.0 -m "django-native-postgres 0.1.0"
git push origin v0.1.0
```

The release workflow builds the package files once, publishes them to
TestPyPI, installs and smoke-tests the TestPyPI package, and then waits for the
`pypi` environment approval.

Review the TestPyPI page and workflow results before approving PyPI. After
approval, the same package files are published to PyPI and attached to the
GitHub release.

## Verify

After publication:

1. Check the PyPI metadata and complete file list.
2. Install the exact Django fork and released package in a clean environment.
3. Run the wheel smoke test and one Django query against PostgreSQL.
4. Confirm that the GitHub release contains the same package files.

The Django fork must be installed explicitly using the command in the README.

## Failed releases

Before PyPI approval, reject the deployment and publish the fix under a new
version. Do not move a published tag to another commit.

If a broken version reaches PyPI, yank it with a clear reason and release a
corrected version. Published package files cannot be replaced.
