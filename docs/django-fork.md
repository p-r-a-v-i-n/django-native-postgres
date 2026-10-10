# Django fork requirement

The native async execution path currently depends on a development fork of
Django. Upstream Django's async ORM methods normally call synchronous ORM
methods through `sync_to_async()`. That boundary prevents a third-party backend
from receiving compiled SQL before synchronous database execution begins.

## What the fork provides

The fork keeps Django's existing query construction and SQL compilation. It
adds an async compiler and backend boundary so an opted-in backend can execute
the compiled SQL and parameters asynchronously.

The execution flow is:

```text
async QuerySet or model method
        |
        v
Django query construction and SQL compiler
        |
        v
DatabaseWrapper async cursor contract
        |
        v
django-native-postgres
```

The fork also provides the async transaction boundary used by
`transaction.atomic()`. Backends that do not opt in keep their existing
synchronous behavior.

## Development dependency

This repository currently selects the fork through `uv`:

```toml
[tool.uv.sources.django]
git = "https://github.com/p-r-a-v-i-n/django.git"
branch = "feature/native-async-backend-contract"
```

This source override is for this repository's development environment. It is
not a portable transitive dependency for applications installing the package
with `pip`.

Before the first package release, the Django dependency must be pinned to an
exact tested commit and the installation guide must explain how applications
install that fork. A mutable branch is not sufficient for a reproducible
release.

## Why the backend does not patch Django at runtime

Runtime monkey-patching or permanent copies of Django's `QuerySet`, compiler,
and model internals would make the backend depend on private implementation
details and require repeated overrides as Django evolves. The fork provides a
single explicit execution contract instead.

The long-term goal is a small official Django backend seam. Until that exists,
the fork is part of this project's tested platform.
