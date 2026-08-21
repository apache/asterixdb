<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
# AsterixDB

Apache AsterixDB — the BDMS query engine, storage layer, and Hyracks distributed runtime. See
`README.md` for an overview.

The tree has two roots:
- `asterixdb/` — the AsterixDB engine (SQL++ compiler, external data, cloud, storage).
- `hyracks-fullstack/` — the Hyracks runtime and shared utilities (`hyracks-util`,
  `hyracks-cloud`, etc.).

Files here carry the **Apache license header** (see `README.md`). Preserve it when editing or
creating files.

This repository is also consumed by downstream extensions, which may check it out inside their own
source tree. Where that is so, the extension's own conventions — coordinating one change across two
Gerrit servers, the order in which such a set is landed, and mirroring merged commits to the fork
the extension actually builds from — live in the parent directory's instructions, which are **not**
auto-loaded from here. Read them before coordinating a change with an extension.

## Code Review

Changes go through **Gerrit** (`asterix-gerrit.ics.uci.edu`), not GitHub pull requests. The `gerrit`
remote points at `ssh://<user>@asterix-gerrit.ics.uci.edu:29418/asterixdb`. If it is missing:

```bash
git remote add gerrit ssh://<user>@asterix-gerrit.ics.uci.edu:29418/asterixdb
```

Push a change (or a new patchset) for review:

```bash
git push gerrit HEAD:refs/for/<branch>
```

Subject convention is `[NO ISSUE][COMP]` (or `[ASTERIXDB-nnnn][COMP]`); where the work is tracked in
a downstream extension's issue tracker, that id goes in an `Ext-ref:` footer rather than the subject.
A backport carries an extra `[BP]` tag — `[ASTERIXDB-3765][COMP][BP] ...` — reuses the original
commit's `Change-Id`, and keeps a `(cherry picked from commit <sha>)` line. Gerrit warns on subjects
over **72** characters here; an extension's own Gerrit may warn sooner, so the shorter limit wins for
a change that spans both. Documentation-only and tooling changes go under whatever standing issue the
extension uses for them.

### Topics: at most one open change per project per topic

A topic (`-o topic=<topic>`) groups the changes of one coordinated change spanning several
repositories. **The tooling that applies a topic across projects fails when a project has more than
one open change carrying the same topic.** So when the work in *this* repo is a stack of several
commits, do not put the shared topic on every commit. Instead:

- Put the topic only on the **leaf** (final) commit of the stack, and leave the preparatory commits
  topic-less; or
- If the stack contains discrete sets of cross-project changes that can be submitted independently,
  give each set its **own intermediate topic**.

Unrelated follow-up work (e.g. a docs tweak noticed along the way) should be pushed as its own
change with **no topic**, or a different one — not added to the coordinated change's topic.

## Commit Messages

**Every commit message must end with the trailer:**

```
Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
```

(naming whichever model actually did the work). This has repeatedly been missed and caught only
after the change was pushed for review, which costs a message-only patchset.

**Write it at commit time, not as a follow-up amend.** The footer must be one contiguous run of
trailers, with the `Co-Authored-By` line adjacent to `Change-Id`:

```
<body>
                                                        <- exactly one blank line
Ext-ref: <downstream-issue>

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Change-Id: I...
```

Two ways the footer gets destroyed, both of which make the `commit-msg` hook mint a **second**
`Change-Id` and silently orphan the change:

- **A blank line between `Change-Id:` and `Co-Authored-By:`** splits the footer, so the last
  paragraph is read as body text.
- **Ending the message with a non-trailer line** — most often
  `(cherry picked from commit <sha>)` on a backport. Put that line in its own paragraph *above* the
  trailer block so the message still ends with a trailer.

If the trailer must be added after the fact, rebuild the whole message rather than appending a line:
capture the `Change-Id` first, strip every `Change-Id:`/`Co-Authored-By:` line, then re-append both
contiguously. Verify before pushing — this must print exactly `1`, matching the original:

```bash
git log -1 --format=%B | grep -c '^Change-Id:'
```

## AI Provenance Annotation

**When you (an AI agent) generate or assist with Java code in this tree, annotate it with
`@AiProvenance`.** This records which model and tool produced the code so the contribution is
auditable. The annotation is defined *here*, in this repository.

- **Annotation**: `org.apache.hyracks.util.annotations.AiProvenance` — source at
  `hyracks-fullstack/hyracks/hyracks-util/src/main/java/org/apache/hyracks/util/annotations/AiProvenance.java`.
  It is on the classpath of every module that depends on `hyracks-util` (effectively all of them).
- **`@Retention(SOURCE)`** — documentation only; stripped at compile time, so no runtime or bytecode
  cost. The repeatable container `@AiProvenances` is `RUNTIME`-retained for tools that scan class
  files.
- **Applies to**: types, methods, constructors, fields, and local variables.

### Attributes

| Attribute | Required | Values |
|-----------|----------|--------|
| `agent` | yes | `AiProvenance.Agent` — the model that did the work (e.g. `CLAUDE_OPUS_5`, `CLAUDE_SONNET_4_6`) |
| `tool` | yes | `AiProvenance.Tool` — the invocation surface (e.g. `CLAUDE_CODE_CLI`, `CLAUDE_CODE_UI`, `GITHUB_COPILOT`) |
| `contributionKind` | no (default `GENERATED`) | `GENERATED`, `ASSISTED`, `REFACTORED`, `TEST_GENERATED`, `DOC_GENERATED` |
| `notes` | no | free-text, e.g. what changed or why |

### How to inject it

1. **Pick the narrowest element** that captures the AI contribution: annotate the **type** only when
   the **whole class** is AI-authored. When AI adds or rewrites individual **methods** (or a
   **field**/**constructor**) within an otherwise human-authored class, annotate each of those
   **methods** — do **not** promote the annotation to the type level.
2. **Match `agent` to the model actually used**, and `tool` to the surface: the `claude` CLI and the
   Claude Code desktop/IDE integrations are `CLAUDE_CODE_CLI`; the Code tab in the Claude web app is
   `CLAUDE_CODE_UI`; Copilot-in-IDE is `GITHUB_COPILOT`.
3. **Choose `contributionKind`** honestly: `GENERATED` (from scratch), `ASSISTED` (human-led,
   AI-suggested), `REFACTORED` (rewriting existing code), `TEST_GENERATED`, `DOC_GENERATED`.
4. **Stack annotations** (it is `@Repeatable`) to record history — e.g. generated by one model then
   refactored by another.
5. **Import style**: either static-import the constants for brevity, or import the type and qualify
   (`AiProvenance.Agent.X`). Both are used in-tree.

If a model you used is missing from the `Agent`/`Tool`/`Provider` enums, **add it to
`AiProvenance.java`** rather than falling back to `OTHER`.

### Example

In-tree usages on this branch include `hyracks-fullstack/hyracks/hyracks-util/.../Span.java`
(type-level, `REFACTORED`), `hyracks-fullstack/hyracks/hyracks-util/.../SpanTest.java`,
`asterixdb/asterix-external-data/.../evaluators/StringJsonParseEval.java`,
`asterixdb/asterix-external-data/.../aws/s3/S3Utils.java` and
`asterixdb/asterix-cloud/.../S3TrustManagerProvider.java`.

```java
import org.apache.hyracks.util.annotations.AiProvenance;

@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI,
        contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Initial implementation")
public final class ExampleHelper { ... }
```
