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

## Tested fork revision

Each backend release is tied to one tested Django fork revision. The current
revision is:

```text
Repository: https://github.com/p-r-a-v-i-n/django
Commit:     312860d0e58e0d54fef10bae7ecf63f537723d17
Version:    6.2.dev20261009125329
```

Install the fork and backend together:

```console
python -m pip install \
    "Django @ git+https://github.com/p-r-a-v-i-n/django.git@312860d0e58e0d54fef10bae7ecf63f537723d17" \
    django-native-postgres
```

The backend's published metadata requires
`Django==6.2.dev20261009125329`. Upstream PyPI does not provide that tested
fork distribution, so installing the backend without supplying the fork fails
dependency resolution rather than silently installing incompatible Django
code.

This repository selects the same exact revision through `uv`:

```toml
[tool.uv.sources.django]
git = "https://github.com/p-r-a-v-i-n/django.git"
rev = "312860d0e58e0d54fef10bae7ecf63f537723d17"
```

The exact commit is also recorded in `uv.lock`. CI, development, wheel smoke
tests, and application installation therefore use the same Django code.

PyPI packages cannot safely embed a mutable Git dependency, and public package
indexes are expected to reject direct URL dependencies in uploaded metadata.
The Git reference is therefore an explicit application installation input,
while the backend publishes a normal exact-version requirement.

The backend also validates the async compiler, cursor, and transaction
contract when Django loads it. Bypassing dependency resolution with an
incompatible Django installation raises `ImproperlyConfigured` instead of
silently returning to synchronous database I/O.

## Why the backend does not patch Django at runtime

Runtime monkey-patching or permanent copies of Django's `QuerySet`, compiler,
and model internals would make the backend depend on private implementation
details and require repeated overrides as Django evolves. The fork provides a
single explicit execution contract instead.

The long-term goal is a small official Django backend seam. Until that exists,
the fork is part of this project's tested platform.
