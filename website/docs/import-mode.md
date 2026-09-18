---
title: Import mode
---

# Import mode

Import mode lets you migrate schemas from another registry while keeping their original
schema IDs and version numbers.

A registry mode controls which write operations Karapace accepts. Karapace supports two:

- `READWRITE` — the default. Karapace assigns the schema ID and the version number,
  deduplicates identical schemas, and enforces the configured compatibility level.
- `IMPORT` — the client supplies the schema ID and the version number, and Karapace honours
  them exactly.

The ID matters because it is embedded in the wire format of every message a producer writes.
Data already on your topics refers to schemas by ID, so a migration that reassigned IDs would
leave that data unreadable.

## Quick start

The examples below use two shell variables:

```bash
SR=http://localhost:8081
CT='Content-Type: application/vnd.schemaregistry.v1+json'
```

A migration into one subject is three requests:

```bash
# 1. put the subject into import mode. It does not have to exist yet
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode/my-subject
# {"mode":"IMPORT"}

# 2. register each schema with the id and version it had in the source registry
curl -s -X POST -H "$CT" \
  -d '{"schema":"{\"type\":\"string\"}","id":1001,"version":5}' \
  $SR/subjects/my-subject/versions
# {"id":1001}

# 3. return the subject to normal operation
curl -s -X DELETE $SR/mode/my-subject
# {"mode":"READWRITE"}
```

The subject now holds version 5, not version 1, and the schema is reachable by its original
ID:

```bash
curl -s $SR/subjects/my-subject/versions    # [5]
curl -s $SR/schemas/ids/1001                # the imported schema
curl -s $SR/subjects/my-subject/versions/1  # 404 {"error_code":40402,"message":"Version 1 not found."}
```

## Setting the mode

The mode can be set globally or per subject, and a subject-level mode always wins over the
global one.

| Request                  | Effect                                                |
| ------------------------ | ----------------------------------------------------- |
| `GET /mode`              | Read the global mode                                  |
| `PUT /mode`              | Set the global mode                                   |
| `GET /mode/{subject}`    | Read the effective mode of a subject                  |
| `PUT /mode/{subject}`    | Set a subject-level mode                              |
| `DELETE /mode/{subject}` | Remove the subject-level mode, falling back to global |

`PUT /mode/{subject}` deliberately does not require the subject to exist, because setting
import mode on a subject that has never been written to is how a migration into a new subject
starts. Such a subject does not appear in `GET /subjects` until it has a live schema, though
it does appear under `GET /subjects?deleted=true`.

Mode writes are handled by the elected primary. Send one to any node and it is forwarded
there with the query string and body intact. If no primary is known yet the request fails
with `50003`. Reads are answered locally by whichever node you ask, so a follower that has
not caught up can briefly report an older mode.

### Reading the effective mode

`GET /mode/{subject}` resolves the mode a subject actually operates under. A subject with no
override of its own already reports the global mode, so `?defaultToGlobal=` changes one case
only: a subject that does not exist.

| Situation                       | Default         | With `?defaultToGlobal=true` |
| ------------------------------- | --------------- | ---------------------------- |
| Subject has its own override    | that override   | that override                |
| Subject exists, has no override | the global mode | the global mode              |
| Subject does not exist          | `404` / `40401` | the global mode              |

```bash
# a subject that does not exist is the only case the flag changes
curl -s "$SR/mode/no-such-subject"
# 404 {"error_code":40401,"message":"Subject 'no-such-subject' not found."}
curl -s "$SR/mode/no-such-subject?defaultToGlobal=false"
# 404, identical to omitting the parameter
curl -s "$SR/mode/no-such-subject?defaultToGlobal=true"
# 200 {"mode":"READWRITE"}

# a subject that exists with no override already falls back to global
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}"}' $SR/subjects/exists/versions
curl -s "$SR/mode/exists"                        # {"mode":"READWRITE"}
curl -s "$SR/mode/exists?defaultToGlobal=true"   # {"mode":"READWRITE"}, no difference

# an override wins either way
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode/imported
curl -s "$SR/mode/imported?defaultToGlobal=true" # {"mode":"IMPORT"}
```

The flag earns its keep while the whole registry is in import mode, when a subject you have
not created yet already reports `IMPORT`:

```bash
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode   # requires an empty registry
curl -s "$SR/mode/not-created-yet?defaultToGlobal=true"   # {"mode":"IMPORT"}
```

:::note
`PUT /mode/{subject}` creates the subject entry, so once you have set a mode on a name that
name no longer exercises the 404 case. Use a fresh name to see it.
:::

## Entering import mode

Import mode can only be entered against an empty target, so that imported IDs and version
numbers cannot collide with existing ones. What counts as the target depends on the scope:

| Request               | Refused when                                 |
| --------------------- | -------------------------------------------- |
| `PUT /mode/{subject}` | that subject has live schemas                |
| `PUT /mode`           | any subject in the registry has live schemas |

A subject-level import therefore only needs that one subject to be empty, while a
registry-wide import needs the whole registry to be empty. One live schema version anywhere,
in any unrelated subject, is enough to refuse `PUT /mode`.

Which means:

- Use `PUT /mode` for a lift and shift into a **fresh** registry, where it produces an exact
  copy of the source including every schema ID.
- Use `PUT /mode/{subject}` to add schemas to a registry that is **already in use**. Only the
  subject you name has to be empty, so the rest of the registry is untouched.

Not everything that looks like a subject counts as existing:

| In the registry                                                      | Counts? |
| -------------------------------------------------------------------- | ------- |
| A subject with at least one live schema version                      | yes     |
| A subject whose every version is soft deleted                        | no      |
| A subject with no schemas at all, created by a mode or config record | no      |

So a name you can see under `GET /subjects?deleted=true` does not necessarily block a global
import, while any name under `GET /subjects` does.

```bash
# subject scope: only the subject in the path has to be empty
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}"}' $SR/subjects/busy/versions
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode/busy
# 422 {"error_code":42205,"message":"Cannot import since found existing subjects"}

# another subject is unaffected by what "busy" holds
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode/empty-subject
# {"mode":"IMPORT"}

# global scope: one live schema anywhere in the registry is enough to refuse it
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode
# 422 {"error_code":42205,"message":"Cannot import since found existing subjects"}
```

:::note
Both scopes return the same message, which does not name a subject. For a subject-level
rejection the subject is the one in the request path; for a global one, any subject with a
live schema is enough.
:::

Pass `?force=true` to skip the check. It skips the check and nothing else: it never deletes,
soft-deletes or moves an existing schema.

```bash
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' "$SR/mode/busy?force=true"
# {"mode":"IMPORT"}
curl -s $SR/subjects/busy/versions   # [1], the existing schema is untouched
```

`?force=true` works on `PUT /mode` the same way, which is how you would import into a
registry that already holds schemas without moving each subject individually.

:::warning
Forcing import mode onto a populated subject means your import has to avoid every version
number and schema ID already in use, or the individual requests are rejected. Prefer
importing into an empty subject.
:::

## Registering schemas in import mode

Add `id` and `version` to the usual request body. Both must be in the range `[1, 2^31-1]`.

```bash
curl -s -X POST -H "$CT" \
  -d '{"schemaType":"AVRO","schema":"{\"type\":\"string\"}","id":1001,"version":5}' \
  $SR/subjects/my-subject/versions
# {"id":1001}
```

Compared to `READWRITE`, import mode changes four things:

- Content-based deduplication is bypassed, so an identical schema does not collapse onto an
  existing registration.
- Version auto-increment is bypassed, so version numbers may have gaps and need not start
  at 1.
- Compatibility checks are skipped entirely. The subject's compatibility level is ignored for
  the duration of the import.
- The requested ID and version are honoured, or the request is rejected. They are never
  silently replaced.

Schema references are still resolved, so import a referenced schema before the schema that
references it.

`version` is optional. Omit it and the next version number is assigned as usual. `id` is not
optional, because its presence is what selects the mode.

## Guardrails

A mistaken import can do damage that no later error message would announce, so each way of
getting it wrong is refused at the point of the request.

### An ID is bound to its content permanently

```bash
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":1001,"version":1}' \
  $SR/subjects/s/versions
# {"id":1001}
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"int\"}","id":1001,"version":2}' \
  $SR/subjects/s/versions
# 422 {"error_code":42205,"message":"Overwrite new schema with id 1001 is not permitted."}
```

Every message already written by a producer carries this ID in its header. Rebinding the ID
would make all of that data deserialize as a different schema, with no error raised at write
or read time and no way to undo it.

### A version cannot be moved to another schema

```bash
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":100,"version":5}' \
  $SR/subjects/s/versions
# {"id":100}
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"int\"}","id":200,"version":5}' \
  $SR/subjects/s/versions
# 422 {"error_code":42205,"message":"Subject 's' version 5 is already registered with schema
#      id 100, overwriting it with schema id 200 is not permitted."}
```

Otherwise version 5 would quietly start resolving to a different schema, while the original
stayed in the global ID map but unreachable through that subject and version. A consumer
pinned to a subject and version would begin reading different bytes.

A soft-deleted version still owns its slot, so the same rejection applies to it. Re-importing
the identical ID, version and schema is allowed and restores the version.

### The ID selects the mode

Supplying an ID outside import mode is refused rather than ignored:

```bash
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":1001}' \
  $SR/subjects/readwrite-subject/versions
# 422 {"error_code":42205,"message":"Subject 'readwrite-subject' is not in IMPORT mode."}
```

Ignoring it would return a server-assigned ID instead, so a migration tool that does not
compare the response would report success while having stored the schema under the wrong
identity.

Omitting an ID inside import mode is refused for the mirror-image reason:

```bash
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}"}' \
  $SR/subjects/importing/versions
# 422 {"error_code":42205,"message":"Subject 'importing' is in IMPORT mode, a schema id is
#      required to register."}
```

Import mode skips compatibility checks, so accepting an ordinary registration there would
turn a subject left in import mode into one that silently accepts breaking changes.

### Out-of-range IDs and versions

Validated before the request reaches the registry:

```bash
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":0,"version":1}' \
  $SR/subjects/s/versions
# 422 {"error_code":42207,"message":"The specified schema id '0' is not valid. Allowed
#      values are between [1, 2^31-1]"}
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":1,"version":0}' \
  $SR/subjects/s/versions
# 422 {"error_code":42202,"message":"The specified version '0' is not valid. Allowed
#      values are between [1, 2^31-1]"}
```

A version of `0` or `-1` would otherwise create a version slot that nothing can address.

## Constraints and rejections

| Situation                                              | Code  | Behaviour                                                  |
| ------------------------------------------------------ | ----- | ---------------------------------------------------------- |
| Subject has live schemas when entering import mode     | 42205 | Rejected unless `force=true`                               |
| `id` or `version` supplied while not in import mode    | 42205 | Rejected                                                   |
| `id` omitted while in import mode                      | 42205 | Rejected                                                   |
| `id` is already registered with different content      | 42205 | Rejected, IDs are immutable                                |
| `version` is already used by a different ID or content | 42205 | Rejected                                                   |
| `id` outside `[1, 2^31-1]`                             | 42207 | Rejected before reaching the registry                      |
| `version` outside `[1, 2^31-1]`                        | 42202 | Rejected before reaching the registry                      |
| Same content already registered under a different ID   | 42207 | Rejected only when `allow_duplicate_schema_ids` is `false` |
| Any mode change while `mode_mutability` is `false`     | 42205 | Rejected                                                   |
| Unsupported mode value                                 | 42204 | Rejected                                                   |

Replaying the exact same `id`, `version` and schema is idempotent and returns the same ID,
which is what makes a failed import safe to retry.

## Operator flags

Both are configuration only and are read at startup, so changing either means a restart.

### `mode_mutability`

`mode_mutability` (`KARAPACE_MODE_MUTABILITY`) defaults to `true`. Set it to `false` and
every `PUT /mode`, `PUT /mode/{subject}` and `DELETE /mode/{subject}` is refused:

```bash
curl -s -X PUT -H "$CT" -d '{"mode":"IMPORT"}' $SR/mode/anything
# 422 {"error_code":42205,"message":"Mode changes are not allowed"}
```

Reads and ordinary registrations are unaffected. It is a cluster-wide switch deciding whether
the feature is available at all, not an access control: no role or permission works around
it. Use the [authorization rules](./authentication.md) to control who may change modes while
the feature is switched on.

:::warning
The switch only blocks mode _changes_. A subject already in import mode stays in import mode:
it keeps honouring `id` and `version` and keeps skipping compatibility checks, and it can no
longer be moved back to `READWRITE` through the API either, because the switch blocks changes
in both directions. Check `GET /mode` and `GET /mode/{subject}` before turning it off.
:::

### `allow_duplicate_schema_ids`

`allow_duplicate_schema_ids` (`KARAPACE_ALLOW_DUPLICATE_SCHEMA_IDS`) defaults to `true`,
which allows one schema to be imported under more than one ID. Set it to `false` and the
second import is refused:

```bash
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":1001,"version":1}' \
  $SR/subjects/s/versions
# {"id":1001}
curl -s -X POST -H "$CT" -d '{"schema":"{\"type\":\"string\"}","id":2001,"version":2}' \
  $SR/subjects/s/versions
# 422 {"error_code":42207,"message":"Schema already registered with id 1001 instead of
#      input id 2001"}
```

Re-importing content under the ID it already owns stays allowed either way, so idempotent
replays are unaffected by the setting.

## Schema IDs are global

Schema IDs are unique per registry, not per subject, and a given ID maps to exactly one
schema for the lifetime of the registry. That has consequences when you consolidate multiple
sources:

- Importing the same ID from two source registries that had bound it to different schemas
  cannot succeed. The second import is rejected with `42205`.
- Importing identical content under two different IDs is allowed by default, because sources
  being merged can legitimately disagree about which ID holds a schema. Both IDs are kept and
  both resolve to the same schema. Set `allow_duplicate_schema_ids` to `false` to refuse it
  instead.

Import one source at a time. If two sources have colliding IDs, either keep them in separate
Karapace clusters, or remap the IDs of one source — which means rewriting the data already
produced against those IDs, since the ID is embedded in every message.

Importing ID _N_ raises the registry's ID counter to _N_. After you leave import mode, new
schemas get IDs above the highest one you imported, so ordinary registrations cannot collide
with imported schemas.

:::warning
Importing an ID close to `2^31-1` exhausts the usable ID space for that registry: every later
schema would need a higher ID.
:::

### When one schema has several IDs

With `allow_duplicate_schema_ids` left at `true`, an import can leave one schema reachable
through more than one ID. Which ID a later registration resolves to then depends on the
subject:

| Request                                                     | ID returned                                      |
| ----------------------------------------------------------- | ------------------------------------------------ |
| `POST` of content the subject already holds                 | the ID that subject recorded, unambiguous        |
| `POST` of that content into a subject that does not hold it | the **earliest registered** of the duplicate IDs |
| `GET /schemas/ids/{id}`                                     | the same schema for either ID                    |
| `GET /subjects/{subject}/versions/{version}`                | the ID stored for that version, unambiguous      |
| `GET /schemas/ids/{id}/versions`                            | only the subjects and versions under that ID     |

The per-subject answer is the one that matters in practice: each subject and version maps to
exactly one ID, so producers and consumers are unaffected. The cross-subject answer is stable,
since it follows the order the records were written to `_schemas` and that order replays
identically, but it is not something to depend on.

## How modes are stored

A mode is not held in a separate store. Setting one appends a record to the internal
`_schemas` topic, alongside the schema and config records:

| Record  | Key                                           | Value                              |
| ------- | --------------------------------------------- | ---------------------------------- |
| Global  | `{"keytype":"MODE","subject":null,"magic":0}` | `{"subject":null,"mode":"IMPORT"}` |
| Subject | `{"keytype":"MODE","subject":"s","magic":0}`  | `{"subject":"s","mode":"IMPORT"}`  |
| Removal | `{"keytype":"MODE","subject":"s","magic":0}`  | `null`                             |

A `subject` of `null` is the global mode. `DELETE /mode/{subject}` writes the third form, a
tombstone, rather than a record saying `READWRITE`. The two are different states: a tombstone
removes the override so the subject follows the global mode, while an explicit `READWRITE`
pins the subject whatever the global mode is. That distinction is what lets one subject be
held back from a registry-wide migration.

Karapace rebuilds its state by replaying the topic from the beginning, so modes survive a
restart. So does the schema ID counter, which is derived from the highest ID ever seen rather
than stored: importing ID 100001 leaves the next ordinary registration at 100002, both during
the import and after any later restart.

## Recovering from a failed import

There is no rollback and no transaction. Every accepted `POST` is durably written to the
`_schemas` topic before the response is returned, so a failure part-way through an import
leaves the subject partially populated and still in import mode. Nothing resets the mode on
its own.

To resume:

1. `GET /mode/{subject}` to confirm the subject is still in import mode.
2. `GET /subjects/{subject}/versions` to see which versions landed.
3. Replay the batch. You do not have to work out which triples are missing: the ones already
   present return their original IDs and the missing ones land, as long as the source data has
   not changed in the meantime.

To start over instead, delete the subject and re-enter import mode:

```bash
curl -s -X DELETE $SR/subjects/my-subject
curl -s -X DELETE "$SR/subjects/my-subject?permanent=true"
```

:::note
Hard-deleting the last version of a subject removes the subject entry, which also discards its
subject-level import mode. Re-issue `PUT /mode/{subject}` before retrying.
:::

Hard-deleting a subject does not free the schema IDs it used: the ID to schema mapping is
global and survives the delete. A retry therefore has to reuse the same IDs for the same
content.

For a large migration, import into a scratch subject or a scratch cluster first and verify the
result before importing into the cluster your applications use.

## Leaving import mode

Return the subject to normal operation with either of:

```bash
curl -s -X PUT -H "$CT" -d '{"mode":"READWRITE"}' $SR/mode/my-subject  # pins the subject
curl -s -X DELETE $SR/mode/my-subject                                  # follows global
```

:::warning
A subject left in import mode still has compatibility checking disabled, and ordinary
registrations that omit an ID are refused. Verify `GET /mode` and `GET /mode/{subject}` at the
end of every migration.
:::

Deleting subjects and versions stays available in import mode, since that is part of migration
cleanup.

## Migration notes

Import mode is modelled on the equivalent feature in other Schema Registry implementations, so
the steps will look familiar if you have migrated before. These are the points worth checking
against your source registry before you start.

- **Modes.** Karapace implements `READWRITE` and `IMPORT`. Any other mode value returns
  `42204`. If your usual procedure freezes the source registry with a read-only mode, there is
  no equivalent here: stop the producers instead.
- **Ranges.** `id` and `version` must be in `[1, 2^31-1]`, checked before the request reaches
  the registry. A source registry holding values outside that range cannot be imported as is.
- **Soft deleted versions.** Entering import mode leaves them in place, so a soft deleted
  version keeps its version slot and its ID binding rather than being cleared. Re-importing
  the same ID and content restores the version.
- **Normalization.** `normalize` is not special cased in import mode, so a schema sent with
  `?normalize=true` is stored in its normalized form. Avro and JSON Schema are unaffected,
  since their canonical form is applied in every mode. Protobuf is: omit the parameter if the
  copy needs to be byte identical to the source.
- **Duplicate IDs.** One schema may be imported under more than one ID by default. Set
  `allow_duplicate_schema_ids` to `false` if your migration requires IDs to stay one to one
  with content.

Authorization for a subject-level mode change is a write permission on that subject. Be aware
that such a change lets the caller bind entries in the registry's global schema ID namespace,
which is a cross-subject effect.

See [Compatibility](./compatibility.md) for the wider picture on migrating from Confluent
Schema Registry.
