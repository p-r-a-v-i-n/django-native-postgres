# Supported PostgreSQL values

Value support is intentionally narrow at this stage. Unsupported result values
encountered during decoding raise an explicit Django `NotSupportedError`; they
are not converted through a silent string fallback.

## Query parameters

| Python value | PostgreSQL binding | Status |
| --- | --- | --- |
| `str` | `TEXT` | Supported |
| `int` | `INT8` | Supported |
| `None` | `UNKNOWN` | Supported when PostgreSQL can infer the target type |

Python integers cross the native boundary as Rust `i64` values. Supported
integers must therefore be between `-(2**63)` and `2**63 - 1`, inclusive.

Python's `bool` is a subclass of `int`. At present, a boolean parameter can be
accepted by the Python boundary and bound as `INT8`; it is not sent as a
PostgreSQL `BOOL`. Do not rely on this behavior. Boolean fields and native
boolean parameters remain unsupported until the boundary handles them
explicitly.

Both positional sequences and named parameter mappings are supported. Django
`%s` and `%(name)s` placeholders are rewritten to PostgreSQL's numbered
placeholder form without rewriting placeholder text inside SQL strings,
quoted identifiers, comments, or dollar-quoted blocks.

Psycopg integer wrapper classes inherit from Python's `int`, so Django's
current integer field adaptation can cross the native boundary. The native
binding is still `INT8`; preserving all original integer-width information is
future work.

## Query results

| PostgreSQL type | Python value | Status |
| --- | --- | --- |
| `TEXT`, `VARCHAR`, `BPCHAR`, `NAME` | `str` | Supported |
| `INT2`, `INT4`, `INT8` | `int` | Supported |
| SQL `NULL` for a supported type | `None` | Supported |

Unsupported result types are rejected while returned rows are decoded. The
current implementation learns column types from the first returned row, so an
empty result does not yet validate or report an unsupported result column.

## Not yet supported

The native parameter and result boundary does not yet provide complete support
for:

- booleans;
- floating-point values;
- `Decimal` and `NUMERIC`;
- binary values and `BYTEA`;
- dates, times, datetimes, time zones, and intervals;
- UUID values;
- JSON and JSONB;
- arrays;
- enums;
- ranges and multiranges;
- network address types; or
- custom PostgreSQL and third-party Django field adapters.

Parameter and result support must be added together with null, overflow,
invalid-value, and round-trip tests. A type should not be documented as
supported merely because PostgreSQL can cast it from text in one query.
