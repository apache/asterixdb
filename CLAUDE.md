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

**Keep downstream detail out of the message body.** This repo's Gerrit and Jira are public, and
upstream of any extension that consumes it, so a commit body describes only what changed *here*:
which property moved, which files, what a merge carried. A downstream extension's product names,
branch-as-product names, issue ids, packaging, licensing and back-compat machinery do not belong
in it. The `Ext-ref:` footer is the sole place a downstream issue id goes — together with the
`(<id>)` that an extension's merge tooling appends to each bullet of a generated missing-commits
list, which is read from that footer and should be left as generated. Where such an id already
sits inside a subject this repo has published, it is history and stays as it is.

A merge commit's body is that generated bullet list and the trailers, nothing more. Reasoning
about a downstream extension's side of a coordinated change belongs in that extension's own half
of it.

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

### Keep the body short

The body says **why** — the defect, the constraint, the reason the obvious approach was rejected.
It is not a narration of the diff: what changed is already in the diff, and restating it hunk by
hunk only leaves two accounts to keep in step. A couple of sentences is usually the right length;
a file-by-file tour is not.

## Code Comments

Comment the **why**, never the **what**. The code already states what it does, and a comment that
merely repeats it is a second rein on the same bit — it adds no control and goes slack the moment
the code moves. Reserve comments for what a reader cannot recover from the code itself: why a
non-obvious approach was chosen, which defect or edge case a guard exists for, an invariant a
caller must uphold, a workaround and the upstream issue it waits on.

Javadoc describing contract — parameters, return values, thrown exceptions, threading expectations
— is not "what" commentary and is welcome.
