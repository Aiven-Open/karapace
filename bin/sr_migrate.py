#!/usr/bin/env python3
r"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

Copy schemas from a registry into Karapace, keeping their schema ids and version numbers.

Commands:
  export        dump a source registry to a file through its REST API
  export-topic  build the same file from a key<TAB>value dump of the source's _schemas topic
  import        load an export into Karapace through IMPORT mode, asking before each step

Examples:
  # a whole registry into a fresh Karapace, exporting from the source as step 1
  bin/sr_migrate.py import --source http://source:8081 --target http://target:8081 \
      --scope global --reserve-subject _migration_id_reservation

  # into a Karapace already in use: only the exported subjects enter IMPORT and have to be empty there
  bin/sr_migrate.py import --source http://source:8081 --target http://target:8081 \
      --scope subject --reserve-subject _migration_id_reservation

  # export first, import later (subject scope is the default)
  bin/sr_migrate.py export --source http://source:8081 --file export.json
  bin/sr_migrate.py import --file export.json --target http://target:8081

  # export from the source's _schemas topic instead of its API
  kcat -C -b kafka:9092 -t _schemas -o beginning -e -q -Z -f '%k\t%s\n' > schemas.log
  bin/sr_migrate.py export-topic --dump schemas.log --file export.json

Auth: SRC_AUTH and DST_AUTH each hold the whole Authorization header value for that registry, the
scheme followed by the credentials, not just the scheme name. Leave them unset when a registry has no
auth. For example:
  export SRC_AUTH="Basic $(printf '%s' 'user:password' | base64)"    # HTTP basic auth
  export DST_AUTH="Bearer $ACCESS_TOKEN"                              # OAuth2 / OIDC access token

--yes answers every prompt with yes, except the one offering force. Re-running the same command
resumes a run that stopped part-way: versions already imported are replayed unchanged.
"""

import argparse
import datetime
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
PLACEHOLDER = '{"type":"record","name":"IdReservation","namespace":"karapace.migration","fields":[]}'


class RegistryError(Exception):
    def __init__(self, method, path, status, body):
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        payload = payload if isinstance(payload, dict) else {}
        self.status = status
        self.error_code = payload.get("error_code")
        self.message = payload.get("message") or body
        code = f" {self.error_code}" if self.error_code else ""
        super().__init__(f"{method} {path} -> {status}{code} {self.message}")


class StepFailed(Exception):
    def __init__(self, message, cause=None):
        super().__init__(message)
        self.cause = cause


class Skip(Exception):
    pass


class Registry:
    def __init__(self, url, auth_env):
        self.url = url.rstrip("/")
        self.auth_env = auth_env
        self.auth = os.environ.get(auth_env)

    def describe_auth(self):
        if not self.auth:
            return f"none (set {self.auth_env} to send an Authorization header)"
        return f"{self.auth.split(' ', 1)[0]} header from {self.auth_env}"

    def call(self, method, path, body=None, allow=()):
        headers = {"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE}
        if self.auth:
            headers["Authorization"] = self.auth
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as e:
            if e.code in allow:
                return None
            raise RegistryError(method, path, e.code, e.read().decode()) from None
        except urllib.error.URLError as e:
            raise RegistryError(method, path, None, f"cannot reach {self.url}: {e.reason}") from None


def quote(subject):
    return urllib.parse.quote(subject, safe="")


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def few(items, limit=5):
    items = list(items)
    shown = ", ".join(str(i) for i in items[:limit])
    return shown + (f" and {len(items) - limit} more" if len(items) > limit else "")


def same_schema(a, b):
    try:
        return json.loads(a) == json.loads(b)
    except ValueError:
        return a.strip() == b.strip()


# Export


def export_api(src):
    # Per subject rather than GET /schemas: Karapace's GET /schemas?deleted=true omits soft-deleted versions.
    schemas = []
    for subject in sorted(src.call("GET", "/subjects?deleted=true")):
        path = f"/subjects/{quote(subject)}/versions"
        live = set(src.call("GET", path, allow=(404,)) or [])
        for version in src.call("GET", f"{path}?deleted=true", allow=(404,)) or []:
            s = src.call("GET", f"{path}/{version}?deleted=true")
            schemas.append(
                {
                    "subject": subject,
                    "version": s["version"],
                    "id": s["id"],
                    "schemaType": s.get("schemaType") or "AVRO",
                    "schema": s["schema"],
                    "references": s.get("references") or [],
                    "deleted": version not in live,
                }
            )
    subject_config = {}
    for subject in sorted({s["subject"] for s in schemas}):
        config = src.call("GET", f"/config/{quote(subject)}", allow=(404,))
        if config:
            subject_config[subject] = config["compatibilityLevel"]
    top_id = max((s["id"] for s in schemas), default=0)
    source_max_id = top_id
    if top_id:
        source_max_id = src.call("GET", f"/schemas/ids/{top_id}?fetchMaxId=true").get("maxId", top_id)
    return {
        "source": {"kind": "api", "location": src.url, "exported_at": now()},
        "global_config": src.call("GET", "/config")["compatibilityLevel"],
        "subject_config": subject_config,
        "source_max_id": source_max_id,
        "schemas": schemas,
    }


def export_topic_dump(path, default_global_config):
    """Rebuild the registry state by replaying a `key<TAB>value` dump of the _schemas topic."""
    versions, subject_config, global_config, seen_ids = {}, {}, None, set()
    with open(path) as f:
        for line_no, line in enumerate(f, 1):
            raw_key, _, raw_value = line.rstrip("\n").partition("\t")
            if raw_key in ("", "NULL", "null"):
                continue
            key = json.loads(raw_key)
            value = None if raw_value in ("", "NULL", "null") else json.loads(raw_value)
            keytype = key.get("keytype")
            if keytype == "SCHEMA":
                slot = (key["subject"], key["version"])
                if value is None:
                    versions.pop(slot, None)
                    continue
                seen_ids.add(value["id"])
                versions[slot] = {
                    "subject": value["subject"],
                    "version": value["version"],
                    "id": value["id"],
                    "schemaType": value.get("schemaType", "AVRO"),
                    "schema": value["schema"],
                    "references": value.get("references") or [],
                    "deleted": value.get("deleted", False),
                }
            elif keytype == "DELETE_SUBJECT":
                for (subject, version), entry in versions.items():
                    if subject == value["subject"] and version <= value["version"]:
                        entry["deleted"] = True
            elif keytype == "CONFIG":
                if key.get("subject") is None:
                    if value:
                        global_config = value["compatibilityLevel"]
                elif value:
                    subject_config[key["subject"]] = value["compatibilityLevel"]
                else:
                    subject_config.pop(key["subject"], None)
            elif keytype not in ("MODE", "NOOP"):
                print(f"line {line_no}: skipping unknown keytype {keytype!r}", file=sys.stderr)

    schemas = list(versions.values())
    hard_deleted = sorted(seen_ids - {s["id"] for s in schemas})
    if hard_deleted:
        print(f"ids no longer bound to any version (hard deleted): {hard_deleted}", file=sys.stderr)
    if global_config is None:
        global_config = default_global_config
        print(f"no global config record in the topic, using {global_config}", file=sys.stderr)
    return {
        "source": {"kind": "topic dump", "location": os.path.abspath(path), "exported_at": now()},
        "global_config": global_config,
        "subject_config": subject_config,
        "source_max_id": max(seen_ids, default=0),
        "hard_deleted_ids": hard_deleted,
        "schemas": schemas,
    }


def describe(dump):
    schemas = dump["schemas"]
    deleted = sum(s["deleted"] for s in schemas)
    subjects = len({s["subject"] for s in schemas})
    top_id = max((s["id"] for s in schemas), default=0)
    return (
        f"{len(schemas)} versions ({deleted} soft deleted) across {subjects} subjects, "
        f"highest id {top_id}, source max id {dump['source_max_id']}"
    )


def write_dump(path, dump):
    with open(path, "w") as f:
        json.dump(dump, f, indent=2)


def cmd_export(args):
    dump = export_api(Registry(args.source, "SRC_AUTH"))
    write_dump(args.file, dump)
    print(f"exported {describe(dump)}")


def cmd_export_topic(args):
    dump = export_topic_dump(args.dump, args.global_config)
    write_dump(args.file, dump)
    print(f"exported {describe(dump)}")


# Import


def import_order(schemas):
    """Id order, with every referenced version moved ahead of its referrers."""
    by_key = {(s["subject"], s["version"]): s for s in schemas}
    seen, order = set(), []

    def visit(schema):
        key = (schema["subject"], schema["version"])
        if key in seen:
            return
        seen.add(key)
        for ref in schema["references"]:
            dep = by_key.get((ref["subject"], ref["version"]))
            if dep is None:
                raise StepFailed(f"{key} references {ref['subject']} v{ref['version']}, which is not in the export")
            visit(dep)
        order.append(schema)

    for schema in sorted(schemas, key=lambda s: (s["id"], s["subject"], s["version"])):
        visit(schema)
    return order


def header(title):
    print(f"\n== {title} " + "=" * max(0, 70 - len(title)))


def kv(key, value):
    print(f"  {key:<28} {value}")


class Migration:
    def __init__(self, args):
        self.args = args
        self.global_scope = args.scope == "global"
        self.dst = Registry(args.target, "DST_AUTH")
        self.src = Registry(args.source, "SRC_AUTH") if args.source else None
        self.force = args.force
        self.dump = None
        self.schemas = []
        self.subjects = []
        self.reserve = None
        self.check = {}
        self.config_plan = []
        self.results = []

    # Prompting and bookkeeping

    def ask(self, question, risky=False):
        if self.args.yes:
            answer = "n (not assumed by --yes)" if risky else "y (--yes)"
            print(f"{question} [y/N] {answer}")
            return not risky
        try:
            return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")
        except EOFError:
            print()
            return False

    def scope_subjects(self):
        return self.subjects + ([self.reserve] if self.reserve else [])

    def source_label(self):
        if self.dump and self.dump.get("source"):
            source = self.dump["source"]
            return f"{source['kind']} {source['location']} (exported {source['exported_at']})"
        return f"registry {self.src.url}" if self.src else f"file {self.args.file}"

    def steps(self):
        load_summary = (
            f"Export every version, soft delete, compatibility level and the ID high-water mark from the source, "
            f"and save it to {self.args.file}."
            if self.src
            else f"Read the export in {self.args.file}."
        )
        return [
            ("Export from source" if self.src else "Load export", load_summary, self.preview_load, self.step_load),
            (
                "Check target",
                "Look for subjects, versions and IDs on the target that clash with the export. Changes nothing.",
                self.preview_check,
                self.step_check,
            ),
            (
                "Enter IMPORT mode",
                "PUT /mode." if self.global_scope else "PUT /mode/{subject} for every subject in the export.",
                self.preview_enter,
                self.step_enter,
            ),
            (
                "Register versions",
                "POST each version with its original ID and version, referenced schemas first.",
                self.preview_register,
                self.step_register,
            ),
            (
                "Reproduce soft deletes",
                "DELETE the versions that are soft deleted in the source.",
                self.preview_soft_delete,
                self.step_soft_delete,
            ),
            (
                "Reserve source max ID",
                "If the source handed out IDs above the highest exported one, hold the highest in --reserve-subject "
                "so the target never hands them out again.",
                self.preview_reserve,
                self.step_reserve,
            ),
            (
                "Apply compatibility levels",
                "Compare the target's compatibility levels with the export and set only those that differ: "
                "PUT /config/{subject}" + (", and PUT /config for the global level." if self.global_scope else "."),
                self.preview_config,
                self.step_config,
            ),
            ("Verify", "Check every version on the target carries its source ID.", None, self.step_verify),
            (
                "Leave IMPORT mode",
                ("PUT /mode READWRITE" if self.global_scope else "DELETE /mode/{subject} for every subject")
                + ", then confirm nothing in scope is still in IMPORT.",
                self.preview_leave,
                self.step_leave,
            ),
        ]

    def run(self):
        steps = self.steps()
        header("Plan")
        for number, (title, summary, _, _) in enumerate(steps, 1):
            print(f"  {number}. {title}\n     {summary}")
        if not self.print_context():
            return 1

        for number, (title, summary, preview, step) in enumerate(steps, 1):
            header(f"Step {number}/{len(steps)}: {title}")
            print(f"  {summary}")
            try:
                for line in preview() if preview else []:
                    print(f"  - {line}")
                if not self.ask(f"Proceed with step {number}?"):
                    self.results.append((title, "stopped", "declined at the prompt"))
                    self.results += [(t, "not run", "") for t, _, _, _ in steps[number:]]
                    break
                detail = step()
                self.results.append((title, "done", detail))
                print(f"  done: {detail}")
            except Skip as e:
                self.results.append((title, "skipped", str(e)))
                print(f"  skipped: {e}")
            except (RegistryError, StepFailed) as e:
                self.results.append((title, "FAILED", str(e)))
                print(f"  FAILED: {e}")
                for hint in self.hints(e):
                    print(f"  hint: {hint}")
                self.results += [(t, "not run", "") for t, _, _, _ in steps[number:]]
                break
            except KeyboardInterrupt:
                print()
                self.results.append((title, "stopped", "interrupted"))
                self.results += [(t, "not run", "") for t, _, _, _ in steps[number:]]
                break
        self.print_summary()
        return 0 if all(status in ("done", "skipped") for _, status, _ in self.results) else 1

    # Context shown before the first step

    def print_context(self):
        header("Connections")
        if self.src:
            kv("source", self.src.url)
            kv("source auth", self.src.describe_auth())
            kv("export file", f"{self.args.file} (written in step 1)")
        else:
            kv("source", f"export file {self.args.file}")
        kv("target", self.dst.url)
        kv("target auth", self.dst.describe_auth())

        header("Settings")
        kv("scope", "global (whole registry)" if self.global_scope else "subject (only subjects in the export)")
        kv("force", "on" if self.force else "off (you are asked if entering IMPORT is refused)")
        kv("reserve subject", self.args.reserve_subject or "none")
        kv("leave IMPORT at the end", "no (--keep-import-mode)" if self.args.keep_import_mode else "yes")
        kv("prompts", "off (--yes)" if self.args.yes else "on")

        try:
            if self.src:
                header("Source registry")
                kv("global compatibility", self.src.call("GET", "/config")["compatibilityLevel"])
                mode = self.src.call("GET", "/mode", allow=(404,))
                kv("global mode", mode["mode"] if mode else "not reported")
        except RegistryError as e:
            print(f"  cannot read the source: {e}")
            return False
        try:
            header("Target registry")
            kv("global mode", self.dst.call("GET", "/mode")["mode"])
            kv("global compatibility", self.dst.call("GET", "/config")["compatibilityLevel"])
            kv("live subjects", len(self.dst.call("GET", "/subjects")))
            kv("subjects incl. soft deleted", len(self.dst.call("GET", "/subjects?deleted=true")))
        except RegistryError as e:
            print(f"  cannot read the target: {e}")
            return False
        print("  not visible over the API, check the target's configuration:")
        print("    mode_mutability             must be true, or every mode change is refused (42205)")
        print("    allow_duplicate_schema_ids  if false, content already held under another ID is refused (42207)")
        return True

    # Step 1: export or load

    def preview_load(self):
        if self.src:
            return [
                f"reads {self.src.url}: /subjects?deleted=true, /subjects/{{subject}}/versions[/{{version}}]"
                "?deleted=true, /config[/{subject}], /schemas/ids/N?fetchMaxId=true",
                f"writes {self.args.file}",
            ]
        return [f"reads {self.args.file}"]

    def step_load(self):
        if self.src:
            self.dump = export_api(self.src)
            write_dump(self.args.file, self.dump)
        else:
            with open(self.args.file) as f:
                self.dump = json.load(f)
        self.schemas = import_order(self.dump["schemas"])
        self.subjects = sorted({s["subject"] for s in self.schemas})
        highest = max((s["id"] for s in self.schemas), default=0)
        if self.dump["source_max_id"] > highest and self.args.reserve_subject:
            self.reserve = self.args.reserve_subject

        kv("source", self.source_label())
        kv("global compatibility", self.dump["global_config"])
        kv("subject compatibility", f"{len(self.dump['subject_config'])} subjects with their own level")
        hard_deleted = self.dump.get("hard_deleted_ids")
        if hard_deleted:
            kv("hard-deleted ids", f"{few(hard_deleted)} (found in the topic, not exportable)")
        source_max = self.dump["source_max_id"]
        if source_max > highest:
            unused = f"id {source_max}" if source_max == highest + 1 else f"ids {highest + 1}..{source_max}"
            print(
                f"  note: the source handed out {unused}, but no exported version uses it: it was hard deleted\n"
                "        (or skipped by an IMPORT). Messages on your topics may still carry it, so the target\n"
                "        must never hand it out again; see step 'Reserve source max ID'"
            )
        return describe(self.dump)

    # Step 2: pre-flight on the target

    def preview_check(self):
        ids = len({s["id"] for s in self.schemas})
        reserved = f", plus id {self.dump['source_max_id']} to be reserved" if self.reserve else ""
        return [
            f"read only: GET /subjects, and GET /schemas/ids/{{id}} for each of {ids} distinct ids "
            f"({len(self.schemas)} versions, some may share an id){reserved}"
        ]

    def step_check(self):
        target_live = set(self.dst.call("GET", "/subjects"))
        target_all = set(self.dst.call("GET", "/subjects?deleted=true"))
        overlap = sorted(set(self.scope_subjects()) & target_live)

        # Content this run writes under each id, checked against what the target already holds there.
        to_write = {}
        for s in self.schemas:
            to_write.setdefault(s["id"], s["schema"])
        if self.reserve:
            # Step 6 writes the placeholder at this id, so a placeholder there already is a replay.
            to_write[self.dump["source_max_id"]] = PLACEHOLDER
        id_same, id_clash = [], []
        for schema_id, schema in sorted(to_write.items()):
            got = self.dst.call("GET", f"/schemas/ids/{schema_id}", allow=(404,))
            if got is not None:
                (id_same if same_schema(got["schema"], schema) else id_clash).append(schema_id)

        by_subject = {}
        for s in self.schemas:
            by_subject.setdefault(s["subject"], []).append(s)
        version_same, version_clash = [], []
        for subject in sorted(set(by_subject) & target_all):
            taken = set(self.dst.call("GET", f"/subjects/{quote(subject)}/versions?deleted=true", allow=(404,)) or [])
            for s in by_subject[subject]:
                if s["version"] in taken:
                    got = self.dst.call("GET", f"/subjects/{quote(subject)}/versions/{s['version']}?deleted=true")
                    (version_same if got["id"] == s["id"] else version_clash).append(f"{subject} v{s['version']}")

        already = self.already_in_import()
        refused = bool(target_live) and not already if self.global_scope else bool(set(overlap) - set(already))
        self.check = {
            "refused": refused,
            "already": already,
            "same": len(version_same),
            "clashes": len(id_clash) + len(version_clash),
        }

        kv("subjects already live", f"{len(overlap)} {few(overlap)}".rstrip())
        if self.global_scope and target_live and not overlap:
            kv("", f"the target holds {len(target_live)} other live subjects")
        if already:
            where = "global" if self.global_scope else f"{len(already)} of {len(self.enter_targets())} subjects"
            kv("already in IMPORT", f"{where}, likely left by an earlier run that stopped part-way")
        kv(
            "entering IMPORT",
            "refused without force (target not empty)"
            if refused
            else "accepted"
            if not already
            else "nothing to enter for those; this run resumes the import",
        )
        kv("ids already present", f"{len(id_same)} same content (replayed unchanged), {len(id_clash)} different content")
        kv("versions already taken", f"{len(version_same)} same id (replayed), {len(version_clash)} different id")
        if id_clash:
            print(f"  clash: ids bound to different content on the target: {few(id_clash)}")
        if version_clash:
            print(f"  clash: versions bound to another id on the target: {few(version_clash)}")
        if self.check["clashes"]:
            print("  these will fail in 'Register versions' with 40901, and force does not help with them")
        return f"{len(overlap)} subjects overlap, {self.check['clashes']} clashes"

    # Step 3: enter IMPORT

    def enter_targets(self):
        return [None] if self.global_scope else self.scope_subjects()

    def preview_enter(self):
        targets = "the global mode" if self.global_scope else f"{len(self.enter_targets())} subjects"
        lines = [f"sets {targets} to IMPORT; compatibility checks are off while in IMPORT"]
        already = self.check.get("already")
        if already:
            where = "the global mode is" if self.global_scope else f"{len(already)} of these subjects are"
            lines.append(f"{where} already in IMPORT, most likely from an earlier run that stopped part-way")
            lines.append("the empty-target check is not repeated for those; if another migration uses this target, answer n")
        if self.check.get("refused") and not self.force:
            lines.append("expected to be refused because the target is not empty; you will be offered force=true")
        return lines

    def already_in_import(self):
        found = []
        for subject in self.enter_targets():
            path = "/mode" if subject is None else f"/mode/{quote(subject)}?defaultToGlobal=true"
            if self.dst.call("GET", path)["mode"] == "IMPORT":
                found.append(subject)
        return found

    def put_import(self, path):
        self.dst.call("PUT", path + ("?force=true" if self.force else ""), {"mode": "IMPORT"})

    def step_enter(self):
        entered = already = 0
        for subject in self.enter_targets():
            path = "/mode" if subject is None else f"/mode/{quote(subject)}"
            read = path if subject is None else f"{path}?defaultToGlobal=true"
            if self.dst.call("GET", read)["mode"] == "IMPORT":
                already += 1
                continue
            try:
                self.put_import(path)
            except RegistryError as e:
                if e.error_code != 40901 or self.force:
                    raise
                print(f"  refused: {e}")
                print("  force=true skips the emptiness check only. It deletes nothing, but every imported ID and")
                print("  version must still be free on the target (see 'Check target').")
                if not self.ask("  Retry with force=true, for this and every remaining subject?", risky=True):
                    raise
                self.force = True
                self.put_import(path)
            entered += 1
        return f"{entered} entered, {already} already in IMPORT, force {'on' if self.force else 'off'}"

    # Step 4: register

    def preview_register(self):
        ids = len({s["id"] for s in self.schemas})
        lines = [f"{len(self.schemas)} versions ({ids} distinct ids) into {len(self.subjects)} subjects will be registered"]
        if self.check.get("same"):
            lines.append(f"{self.check['same']} of them are already on the target and are replayed unchanged")
        if self.check.get("clashes"):
            lines.append(f"{self.check['clashes']} are expected to fail (see 'Check target')")
        return lines

    def register(self, subject, version, schema_id, schema_type, schema, references):
        body = {"schema": schema, "schemaType": schema_type, "id": schema_id, "version": version}
        if references:
            body["references"] = references
        got = self.dst.call("POST", f"/subjects/{quote(subject)}/versions", body)["id"]
        if got != schema_id:
            raise StepFailed(f"{subject} v{version}: sent id {schema_id}, registry returned {got}")

    def step_register(self):
        total = len(self.schemas)
        for done, s in enumerate(self.schemas):
            try:
                self.register(s["subject"], s["version"], s["id"], s["schemaType"], s["schema"], s["references"])
            except RegistryError as e:
                raise StepFailed(f"{s['subject']} v{s['version']} id {s['id']} ({done}/{total} registered): {e}", e)
            if total > 200 and (done + 1) % 100 == 0:
                print(f"  {done + 1}/{total}")
        return f"{total} versions registered"

    # Step 5: soft deletes

    def soft_deleted(self):
        return [s for s in reversed(self.schemas) if s["deleted"]]

    def preview_soft_delete(self):
        deleted = self.soft_deleted()
        if not deleted:
            raise Skip("no soft-deleted versions in the export")
        names = few(f"{s['subject']} v{s['version']}" for s in deleted)
        return [f"{len(deleted)} versions will be soft deleted: {names}"]

    def step_soft_delete(self):
        deleted = self.soft_deleted()
        for s in deleted:
            self.dst.call("DELETE", f"/subjects/{quote(s['subject'])}/versions/{s['version']}", allow=(404,))
        return f"{len(deleted)} versions soft deleted"

    # Step 6: reserve the source's max id

    def preview_reserve(self):
        highest = max((s["id"] for s in self.schemas), default=0)
        source_max = self.dump["source_max_id"]
        if source_max <= highest:
            raise Skip("the source's max id is the highest exported id, nothing to reserve")
        if not self.reserve:
            raise Skip(
                f"WARNING: the source handed out ids up to {source_max} but --reserve-subject was not given, "
                f"so the target may hand out ids {highest + 1}..{source_max} again"
            )
        return [f"imports a placeholder at id {source_max} into {self.reserve} v1, then soft deletes it"]

    def step_reserve(self):
        source_max = self.dump["source_max_id"]
        self.register(self.reserve, 1, source_max, "AVRO", PLACEHOLDER, [])
        self.dst.call("DELETE", f"/subjects/{quote(self.reserve)}/versions/1", allow=(404,))
        return f"id {source_max} held in {self.reserve}"

    # Step 7: compatibility levels

    def config_changes(self):
        """(subject, current, wanted) for each level the target doesn't already have; subject None is global."""
        changes = []
        if self.global_scope:
            current = self.dst.call("GET", "/config")["compatibilityLevel"]
            if current != self.dump["global_config"]:
                changes.append((None, current, self.dump["global_config"]))
        for subject, level in sorted(self.dump["subject_config"].items()):
            got = self.dst.call("GET", f"/config/{quote(subject)}", allow=(404,))
            current = got["compatibilityLevel"] if got else None
            if current != level:
                changes.append((subject, current, level))
        return changes

    def preview_config(self):
        self.config_plan = self.config_changes()
        lines = []
        for subject, current, wanted in self.config_plan[:10]:
            name = "global" if subject is None else subject
            note = " (replaces a level the target set itself)" if subject is not None and current else ""
            lines.append(f"{name}: {current or 'not set'} -> {wanted}{note}")
        if len(self.config_plan) > 10:
            lines.append(f"and {len(self.config_plan) - 10} more")
        if not self.global_scope:
            target_global = self.dst.call("GET", "/config")["compatibilityLevel"]
            if target_global != self.dump["global_config"]:
                lines.append(
                    f"global stays {target_global} on the target (source has {self.dump['global_config']}); "
                    "subject scope does not change it"
                )
        if not self.config_plan:
            details = "".join(f"; {line}" for line in lines)
            raise Skip(f"the target already has every compatibility level in the export{details}")
        return lines

    def step_config(self):
        for subject, _, wanted in self.config_plan:
            path = "/config" if subject is None else f"/config/{quote(subject)}"
            self.dst.call("PUT", path, {"compatibility": wanted})
        return f"{len(self.config_plan)} levels set"

    # Step 8: verify

    def step_verify(self):
        wrong = []
        for s in self.schemas:
            got = self.dst.call("GET", f"/subjects/{quote(s['subject'])}/versions/{s['version']}?deleted=true")["id"]
            if got != s["id"]:
                wrong.append(f"{s['subject']} v{s['version']} has id {got}, expected {s['id']}")
        if wrong:
            raise StepFailed(f"{len(wrong)} versions carry the wrong id: {few(wrong)}")
        return f"{len(self.schemas)} versions carry their source id"

    # Step 9: leave IMPORT

    def preview_leave(self):
        if self.args.keep_import_mode:
            raise Skip("--keep-import-mode: the target stays in IMPORT for a later catch-up pass")
        if self.global_scope:
            return ["global mode back to READWRITE"]
        return [f"removes the override on {len(self.scope_subjects())} subjects, which then follow the global mode"]

    def step_leave(self):
        if self.global_scope:
            self.dst.call("PUT", "/mode", {"mode": "READWRITE"})
        else:
            for subject in self.scope_subjects():
                self.dst.call("DELETE", f"/mode/{quote(subject)}", allow=(404,))
        global_mode, stuck = self.mode_state()
        if global_mode == "IMPORT" or stuck:
            raise StepFailed(f"still in IMPORT: global {global_mode}, subjects {few(stuck) or 'none'}")
        return "nothing in scope is in IMPORT"

    # Failure hints and summary

    def hints(self, error):
        cause = error.cause if isinstance(error, StepFailed) and error.cause else error
        code = getattr(cause, "error_code", None)
        status = getattr(cause, "status", None)
        message = getattr(cause, "message", "") or ""
        hints = []
        if isinstance(cause, RegistryError) and status is None:
            hints.append("the registry could not be reached; check the URL, network and TLS, then re-run")
        elif status in (401, 403):
            hints.append("check SRC_AUTH / DST_AUTH: reading the source, and writing the subjects and Config: on the target")
        elif code == 40901 and "Cannot import" in message:
            hints.append("the target is not empty for this scope. Re-run with --force to enter IMPORT anyway;")
            hints.append("force skips the emptiness check only and deletes nothing, but IDs and versions must be free")
        elif code == 40901:
            hints.append("the target already holds this ID or version with other content; --force does not help.")
            hints.append("resolve the clash shown in 'Check target', then re-run")
        elif code == 42205 and "not allowed" in message:
            hints.append("mode_mutability is false on the target; restart it with KARAPACE_MODE_MUTABILITY=true")
        elif code == 42205:
            hints.append("the subject is not in IMPORT mode (was the mode changed during the run?); re-run to enter it")
        elif code == 42207 and "already registered" in message:
            hints.append("allow_duplicate_schema_ids is false on the target and this content already has another ID")
        elif code in (42202, 42207):
            hints.append("the ID or version is outside [1, 2^31-1] and cannot be imported as is")
        elif status == 422:
            hints.append("the target rejected the schema; if it references another schema, check that one imported")
        hints.append("re-running the same command resumes: versions already imported are replayed unchanged")
        return hints

    def mode_state(self):
        global_mode = self.dst.call("GET", "/mode")["mode"]
        stuck = []
        for subject in self.scope_subjects():
            got = self.dst.call("GET", f"/mode/{quote(subject)}", allow=(404,))
            if got and got["mode"] == "IMPORT":
                stuck.append(subject)
        return global_mode, stuck

    def print_summary(self):
        header("Summary")
        kv("source", self.source_label())
        kv("target", self.dst.url)
        if self.dump:
            kv("export", describe(self.dump))
        print()
        for number, (title, status, detail) in enumerate(self.results, 1):
            print(f"  {number}. {title:<28} {status:<8} {detail}")
        print()
        try:
            global_mode, stuck = self.mode_state()
        except RegistryError as e:
            kv("target mode", f"unknown: {e}")
            return
        kv("target global mode", global_mode)
        if global_mode == "IMPORT" and not self.global_scope:
            kv("", "the global mode is IMPORT, so every subject without an override is in IMPORT too")
        kv("subjects still in IMPORT", f"{len(stuck)} {few(stuck)}".rstrip())
        if global_mode == "IMPORT" or stuck:
            print("\n  The target is still in IMPORT: compatibility checks are off and ordinary registrations are refused.")
            print("  Re-run the same command to resume, or leave IMPORT by hand:")
            if global_mode == "IMPORT":
                body = '{"mode":"READWRITE"}'
                print(f"    curl -X PUT -H 'Content-Type: {CONTENT_TYPE}' -d '{body}' {self.dst.url}/mode")
            if stuck:
                print(f"    curl -X DELETE {self.dst.url}/mode/<subject>   # for each subject above")


def cmd_import(args):
    if not args.file:
        args.file = "export.json"
    sys.exit(Migration(args).run())


IMPORT_EXAMPLES = r"""
examples, with a source registry on localhost:8081 and the target Karapace on localhost:8083:

  # migrate a whole registry into an empty Karapace: export from the source as step 1, then import
  python3 bin/sr_migrate.py import --source http://localhost:8081 --target http://localhost:8083 \
      --scope global --reserve-subject _migration_id_reservation

  # migrate into a Karapace already in use: only the exported subjects enter IMPORT and must be empty,
  # every other subject and the target's global compatibility level are left alone
  python3 bin/sr_migrate.py import --source http://localhost:8081 --target http://localhost:8083 \
      --scope subject --reserve-subject _migration_id_reservation

  # import an export made earlier (subject scope, the default)
  python3 bin/sr_migrate.py import --file export.json --target http://localhost:8083

  # the same without prompts, for scripts; force is never assumed
  python3 bin/sr_migrate.py import --file export.json --target http://localhost:8083 --yes

  # with auth on the target
  DST_AUTH="Basic $(printf 'admin:secret' | base64)" \
      python3 bin/sr_migrate.py import --file export.json --target https://karapace.example.com
"""

EXPORT_EXAMPLES = """\
example:
  python3 bin/sr_migrate.py export --source http://localhost:8081 --file export.json
"""

EXPORT_TOPIC_EXAMPLES = r"""
example, with Kafka on localhost:9092 and the source's topic named _schemas:
  kcat -C -b localhost:9092 -t _schemas -o beginning -e -q -Z -f '%k\t%s\n' > schemas.log
  python3 bin/sr_migrate.py export-topic --dump schemas.log --file export.json
"""


def main():
    raw = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[1], formatter_class=raw)
    commands = parser.add_subparsers(dest="command", required=True)

    exp = commands.add_parser("export", help="dump a source registry to a file", epilog=EXPORT_EXAMPLES, formatter_class=raw)
    exp.add_argument("--source", required=True, help="source registry URL, e.g. http://localhost:8081")
    exp.add_argument("--file", required=True, help="where to write the export, e.g. export.json")
    exp.set_defaults(run=cmd_export)

    top = commands.add_parser(
        "export-topic",
        help="build the export from a key<TAB>value dump of _schemas",
        epilog=EXPORT_TOPIC_EXAMPLES,
        formatter_class=raw,
    )
    top.add_argument(
        "--dump", required=True, help="key<TAB>value dump of the source's _schemas topic, from kcat or the console consumer"
    )
    top.add_argument("--file", required=True, help="where to write the export, e.g. export.json")
    top.add_argument("--global-config", default="BACKWARD", help="used when the topic has no global config record")
    top.set_defaults(run=cmd_export_topic)

    imp = commands.add_parser(
        "import",
        help="load an export into Karapace step by step, asking before each step",
        epilog=IMPORT_EXAMPLES,
        formatter_class=raw,
    )
    imp.add_argument("--target", required=True, help="target Karapace URL")
    imp.add_argument("--file", help="export to load, or with --source where to save it (default export.json)")
    imp.add_argument("--source", help="export from this registry as step 1 instead of loading an existing file")
    imp.add_argument(
        "--scope",
        choices=("global", "subject"),
        default="subject",
        help="global: PUT /mode, needs an empty registry. subject (default): PUT /mode/{subject} per subject",
    )
    imp.add_argument("--force", action="store_true", help="enter IMPORT even if the target is not empty")
    imp.add_argument("--reserve-subject", help="subject used to hold the source's max id when it exceeds the export")
    imp.add_argument("--keep-import-mode", action="store_true", help="stay in IMPORT, for a later catch-up pass")
    imp.add_argument("--yes", action="store_true", help="do not prompt; risky choices such as force are never assumed")
    imp.set_defaults(run=cmd_import)

    args = parser.parse_args()
    if args.command == "import" and not args.file and not args.source:
        imp.error("give --file, or --source to export first")
    args.run(args)


if __name__ == "__main__":
    main()
