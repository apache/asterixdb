/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.util.iceberg;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundExtract;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Rewrites predicates on shredded {@code VARIANT} sub-fields so Iceberg can use them for data-file pruning.
 * <p>
 * The filter builder turns a nested field access into a dotted column name (e.g. {@code "variant_field.status"}).
 * That is a valid reference for a real Iceberg struct, but a variant is a single opaque {@code VariantType} in the
 * schema, so {@code status} cannot be bound and the predicate is unusable as written. Iceberg
 * instead expresses a variant sub-field with an <em>extract term</em>:
 * {@code Expressions.equal(Expressions.extract("variant_field", "$.status", "string"), value)}. Bounds for variant
 * subcolumns are stored in the manifest as a Variant object keyed by normalized JSON path, and Iceberg's
 * {@code InclusiveMetricsEvaluator} understands extract terms, so a rewritten predicate prunes whole data files at
 * scan-planning time.
 * <p>
 * This rewrite must run where the Iceberg {@link Schema} is available (the reader factory), because only the schema
 * says which column is a variant. Rewriting is best-effort and structure-preserving: predicates that are not variant
 * sub-field references, or whose literal type cannot be mapped to a supported extract type, are returned unchanged.
 * <p>
 * A predicate the rewrite declines is returned untouched, which means it still carries the dotted name — and a dotted
 * name into a variant <em>cannot be bound</em>: Iceberg raises
 * {@code ValidationException: Cannot find field 'v.x' in struct: ...}, failing scan planning rather than quietly
 * skipping the optimization. Callers must therefore run {@link #withoutVariantSubFieldPredicates} over the result
 * before handing it to {@code TableScan.filter(..)}. That is a correctness requirement, not a tuning step.
 * <p>
 * <b>Path syntax, and why the segments matter.</b> Iceberg's {@code PathUtil.parse} — which
 * {@code Expressions.extract} consumes — rejects the bracket form outright on 1.11.0 ({@code Unsupported path, contains
 * bracket}), splits on {@code .}, and validates each segment against RFC 9535's {@code member-name-shorthand} rule. So
 * the only expressible path is dot-notation rooted at {@code $} (e.g. {@code $.a.b}), even though the manifest bound
 * object's own keys use the normalized bracket form.
 * <p>
 * That makes a dot-joined reference name <em>ambiguous</em>: a sub-field literally named {@code "a.b"} and a nested
 * {@code a -> b} both render as {@code "variant_field.a.b"}, and splitting the name back apart silently picks the
 * nested reading. Pruning on the wrong sub-field's bounds drops matching rows — a wrong answer, not a lost
 * optimization. The rewrite therefore works from the <em>unjoined</em> segments the filter builder recorded
 * (see {@code IcebergTableFilterEvaluatorFactory#getFilterPathSegments}) and declines whenever they are unknown,
 * ambiguous, or contain a segment that RFC 9535 shorthand cannot express. Nesting keeps its pushdown; only genuinely
 * inexpressible names lose it.
 *
 * <h2>TODO(iceberg-15384): part of this class is a workaround and must be deleted on upgrade</h2>
 *
 * The rewrite itself is permanent — it is how a variant sub-field predicate is expressed, and nothing upstream
 * replaces it. What is temporary is the machinery that exists only because Iceberg 1.11.0 cannot carry an extract
 * term through {@code TableScan}, which forces us to push a weakened filter and evaluate the extract part ourselves.
 * <p>
 * Remove the following once <a href="https://github.com/apache/iceberg/pull/15384">apache/iceberg#15384</a> is
 * <em>merged upstream <b>and</b> included in the Iceberg release this build depends on</em> (1.11.0 at the time of
 * writing — merged-but-unreleased is not enough, the defect is still in the jar we compile and run against):
 * <ul>
 * <li>{@link #containsExtractTerm(Expression)}, {@link #withoutExtractTerms(Expression)},
 * {@link #extractTermColumns(Expression)} and its helper {@code collectExtractColumns}, and
 * {@link #toBoundKey(String)} — all exist only to split the filter and to look bounds up by hand;</li>
 * <li>{@code segmentsExpressibleInDotNotation} and its callers — #15384 adds bracket-notation paths, so segments
 * needing escaping become expressible and those sub-fields <em>gain</em> pushdown rather than being refused. The
 * segment-based resolution itself stays: brackets remove the need to <em>decline</em> a dotted name, not the need to
 * know the segments in the first place;</li>
 * <li>switch path building in {@code buildExtractPredicate} from dot notation to the bracket form.</li>
 * </ul>
 * {@link VariantBoundsEvaluator}'s class javadoc holds the authoritative, cross-file removal checklist; every site
 * carries the marker {@code iceberg-15384} so {@code grep -rn "iceberg-15384"} finds them all.
 * <p>
 * <b>Not part of that removal:</b> {@link #containsVariantSubFieldPredicate} and
 * {@link #withoutVariantSubFieldPredicates} are permanent. They exist because a dotted variant reference cannot bind
 * at all, which #15384 does not change — and the rewrite will still decline some shapes afterwards (a sub-field
 * {@code IS NULL} has no literal to type an extract with, whatever the path syntax). Deleting them along with the
 * workaround would reintroduce a failed query, not merely a lost optimization.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Rewrites dotted predicates on VARIANT sub-fields into Iceberg extract terms so manifest "
        + "sub-field bounds can prune data files; leaves anything it cannot rewrite untouched")
public final class VariantPredicateRewriter {

    /**
     * RFC 9535's {@code member-name-shorthand} production, mirroring Iceberg's own
     * {@code PathUtil.RFC9535_MEMBER_NAME_SHORTHAND} (which is private). A segment matching this can be written after a
     * dot; anything else — a name containing a dot, a space, a dash, a quote, a bracket — requires the bracket form
     * {@code $['a.b']}, which Iceberg 1.11.0 rejects.
     */
    private static final Pattern RFC9535_MEMBER_NAME_SHORTHAND =
            Pattern.compile("[A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]"
                    + "[0-9A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]*");

    private VariantPredicateRewriter() {
    }

    /**
     * Returns {@code expression} with every predicate that references a shredded variant sub-field rewritten into an
     * equivalent {@code Expressions.extract(..)} predicate. Returns the input unchanged when there is nothing to
     * rewrite (no schema, no variant columns, or no matching predicate).
     */
    public static Expression rewrite(Expression expression, Schema schema,
            Map<String, List<String>> pathSegmentsByName) {
        if (expression == null || schema == null || !hasVariantColumn(schema)) {
            return expression;
        }
        try {
            return rewriteNode(expression, schema, pathSegmentsByName);
        } catch (RuntimeException e) {
            // Never let an optimization attempt break planning: fall back to the original predicate.
            return expression;
        }
    }

    /**
     * Rewrites while <em>assuming every dot in a reference name is nesting</em> — which is exactly the assumption that
     * is unsound in production, because a variant sub-field may itself be named {@code "a.b"}.
     *
     * Test seam only. Production must call
     *             {@link #rewrite(Expression, Schema, Map)} with the unjoined segments the filter builder recorded, so
     *             a dotted field name is distinguishable from nesting.
     */
    static Expression rewriteAssumingNesting(Expression expression, Schema schema) {
        return rewrite(expression, schema, null);
    }

    /**
     * Whether the schema contains a variant ANYWHERE, not just as a top-level column: a variant can sit inside a
     * struct, and a schema whose only variant is nested must still reach the rewrite below.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Looks for variants at any depth; scanning only top-level columns skipped the rewrite entirely for a variant nested in a struct")
    private static boolean hasVariantColumn(Schema schema) {
        return containsVariant(schema.asStruct());
    }

    private static boolean containsVariant(Type type) {
        if (type.typeId() == Type.TypeID.VARIANT) {
            return true;
        }
        if (!type.isNestedType()) {
            return false;
        }
        for (NestedField field : type.asNestedType().fields()) {
            if (containsVariant(field.type())) {
                return true;
            }
        }
        return false;
    }

    private static Expression rewriteNode(Expression expression, Schema schema,
            Map<String, List<String>> pathSegmentsByName) {
        if (expression instanceof And) {
            And and = (And) expression;
            return Expressions.and(rewriteNode(and.left(), schema, pathSegmentsByName),
                    rewriteNode(and.right(), schema, pathSegmentsByName));
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            return Expressions.or(rewriteNode(or.left(), schema, pathSegmentsByName),
                    rewriteNode(or.right(), schema, pathSegmentsByName));
        } else if (expression instanceof Not) {
            return Expressions.not(rewriteNode(((Not) expression).child(), schema, pathSegmentsByName));
        } else if (expression instanceof UnboundPredicate) {
            return rewritePredicate((UnboundPredicate<?>) expression, schema, pathSegmentsByName);
        }
        // Bound predicates and the alwaysTrue/alwaysFalse singletons need no rewriting.
        return expression;
    }

    private static Expression rewritePredicate(UnboundPredicate<?> predicate, Schema schema,
            Map<String, List<String>> pathSegmentsByName) {
        String name = referenceName(predicate);
        if (name == null) {
            return predicate;
        }
        // The reference name is a dot-joined string and that join is lossy: a sub-field named "a.b" and a nested
        // a -> b both render as "a.b". Resolve the ORIGINAL segments instead of splitting the name back apart, so the
        // two cannot be confused. No entry (or a conflicting one) means we cannot tell them apart -> do not rewrite.
        List<String> segments = pathSegmentsByName == null ? splitPath(name) : pathSegmentsByName.get(name);
        if (segments == null || segments.size() < 2) {
            return predicate; // unknown, ambiguous, or not a nested reference at all
        }
        // Find the variant the path goes through. It is not always the first segment: a variant can sit inside a
        // struct, where "st.v.x" means column "st.v" with sub-path "x". Walk the schema segment by segment rather than
        // asking findField for each dotted prefix, for two reasons: a dotted lookup would itself be ambiguous with a
        // column literally named "st.v", and it would happily resolve THROUGH a list or map ("arr.element.x"), whose
        // element bounds carry existential semantics this evaluator does not model. Only struct nesting is followed;
        // anything else declines and takes the ordinary, unpruned path. At least one segment must remain after the
        // variant, or this references the whole variant rather than a sub-field of it.
        int subPathStart = -1;
        Type enclosing = schema.asStruct();
        for (int i = 0; i < segments.size() - 1; i++) {
            if (!(enclosing instanceof Types.StructType)) {
                break; // reached a list, map or primitive: not a struct-nested variant
            }
            NestedField candidate = ((Types.StructType) enclosing).field(segments.get(i));
            if (candidate == null) {
                break;
            }
            if (candidate.type().typeId() == Type.TypeID.VARIANT) {
                subPathStart = i + 1;
                break;
            }
            enclosing = candidate.type();
        }
        if (subPathStart < 0) {
            return predicate; // a real struct, a list/map element, or unknown: not ours to rewrite
        }
        String column = String.join(".", segments.subList(0, subPathStart));

        Object value = literalValue(predicate);
        if (value == null) {
            // No literal to type the extract with (e.g. isNull/notNull on a sub-field): a variant sub-field's
            // null-ness is not derivable from manifest bounds, so leave it alone.
            return predicate;
        }
        String extractType = extractTypeFor(value);
        if (extractType == null) {
            return predicate;
        }

        List<String> subPath = segments.subList(subPathStart, segments.size());
        if (!segmentsExpressibleInDotNotation(subPath)) {
            // Iceberg 1.11.0's PathUtil.parse rejects the bracket form outright ("Unsupported path, contains
            // bracket") and validates each dot-separated segment against RFC 9535's member-name-shorthand rule, so a
            // segment needing escaping cannot be expressed as a path at all. Refuse rather than prune against the
            // wrong sub-field's bounds. apache/iceberg#15384 adds bracket paths and lifts this restriction.
            return predicate;
        }
        String path = "$." + String.join(".", subPath);
        return buildExtractPredicate(predicate.op(), column, path, extractType, value, predicate);
    }

    private static Expression buildExtractPredicate(Expression.Operation op, String column, String path, String type,
            Object value, UnboundPredicate<?> original) {
        switch (op) {
            case EQ:
                return Expressions.equal(Expressions.extract(column, path, type), value);
            case NOT_EQ:
                return Expressions.notEqual(Expressions.extract(column, path, type), value);
            case LT:
                return Expressions.lessThan(Expressions.extract(column, path, type), value);
            case LT_EQ:
                return Expressions.lessThanOrEqual(Expressions.extract(column, path, type), value);
            case GT:
                return Expressions.greaterThan(Expressions.extract(column, path, type), value);
            case GT_EQ:
                return Expressions.greaterThanOrEqual(Expressions.extract(column, path, type), value);
            default:
                // STARTS_WITH, IN, and the rest are not supported on extract terms here; keep the original.
                return original;
        }
    }

    /** @return the predicate's reference name, or {@code null} if it does not have a simple named reference. */
    private static String referenceName(UnboundPredicate<?> predicate) {
        try {
            return predicate.ref().name();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** @return the predicate's single literal value, or {@code null} when it has none (unary predicates). */
    private static Object literalValue(UnboundPredicate<?> predicate) {
        try {
            return predicate.literal() == null ? null : predicate.literal().value();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Maps a literal's Java type to the Iceberg type name {@code Expressions.extract} accepts. Only types whose
     * variant physical representation is unambiguous are mapped; anything else yields {@code null} so the predicate is
     * left untouched. Notably date/time literals are excluded: the filter builder normalizes them to int/long, which
     * would be compared against a DATE/TIME-typed variant sub-field under the wrong type.
     */
    private static String extractTypeFor(Object value) {
        if (value instanceof CharSequence) {
            return "string";
        } else if (value instanceof Boolean) {
            return "boolean";
        } else if (value instanceof Integer) {
            return "int";
        } else if (value instanceof Long) {
            return "long";
        } else if (value instanceof Float) {
            return "float";
        } else if (value instanceof Double) {
            return "double";
        }
        return null;
    }

    /**
     * @return {@code true} if {@code expression} contains at least one extract term, i.e. it cannot be handed to
     *         {@code TableScan.filter(..)} on Iceberg 1.11.0 (see {@link #withoutExtractTerms}).
     *
     * @implNote TODO(iceberg-15384): delete once apache/iceberg#15384 ships — extract terms become safe to push
     *           straight into {@code TableScan.filter(..)}, so nothing needs to detect them.
     *           https://github.com/apache/iceberg/pull/15384
     */
    public static boolean containsExtractTerm(Expression expression) {
        if (expression instanceof And) {
            And and = (And) expression;
            return containsExtractTerm(and.left()) || containsExtractTerm(and.right());
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            return containsExtractTerm(or.left()) || containsExtractTerm(or.right());
        } else if (expression instanceof Not) {
            return containsExtractTerm(((Not) expression).child());
        } else if (expression instanceof UnboundPredicate) {
            return ((UnboundPredicate<?>) expression).term() instanceof UnboundExtract;
        }
        return false;
    }

    /**
     * True if {@code expression} still references a VARIANT sub-field with a plain dotted name — that is, a predicate
     * {@link #rewrite} declined to convert into an extract term.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Detects dotted VARIANT sub-field references left behind by a declined rewrite; such a reference cannot bind and would fail scan planning")
    public static boolean containsVariantSubFieldPredicate(Expression expression, Schema schema) {
        if (schema == null) {
            return false;
        }
        if (expression instanceof And) {
            And and = (And) expression;
            return containsVariantSubFieldPredicate(and.left(), schema)
                    || containsVariantSubFieldPredicate(and.right(), schema);
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            return containsVariantSubFieldPredicate(or.left(), schema)
                    || containsVariantSubFieldPredicate(or.right(), schema);
        } else if (expression instanceof Not) {
            return containsVariantSubFieldPredicate(((Not) expression).child(), schema);
        } else if (expression instanceof UnboundPredicate) {
            return isVariantSubFieldReference((UnboundPredicate<?>) expression, schema);
        }
        return false;
    }

    /** Whether this predicate is a dotted reference whose root column is a VARIANT (so it cannot be bound). */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Leaf test shared by containsVariantSubFieldPredicate and withoutVariantSubFieldPredicates")
    private static boolean isVariantSubFieldReference(UnboundPredicate<?> predicate, Schema schema) {
        if (predicate.term() instanceof UnboundExtract) {
            return false; // already an extract term; handled by the iceberg-15384 split
        }
        String name = referenceName(predicate);
        if (name == null || name.indexOf('.') <= 0) {
            return false;
        }
        // Every dotted prefix, not just the root: a variant can sit inside a struct, and "st.v.x" is as unbindable as
        // "v.x". Checking only the root would leave those to the reader factory's fail-safe, which drops the whole
        // filter and loses pushdown on the ordinary columns ANDed with it. The full name counts too, since a bare
        // reference to a nested variant ("st.v") does not bind either.
        for (int end = name.indexOf('.'); end > 0; end = name.indexOf('.', end + 1)) {
            if (isVariantField(schema, name.substring(0, end))) {
                return true;
            }
        }
        return isVariantField(schema, name);
    }

    private static boolean isVariantField(Schema schema, String name) {
        NestedField field = schema.findField(name);
        return field != null && field.type().typeId() == Type.TypeID.VARIANT;
    }

    /**
     * Removes every predicate that still references a VARIANT sub-field by dotted name, so the expression can be pushed
     * to {@code TableScan.filter(..)}.
     * <p>
     * <b>A correctness guard, not an optimization.</b> A dotted reference into a variant cannot be bound — Iceberg
     * raises {@code ValidationException: Cannot find field 'v.x' in struct} — so pushing one fails scan planning
     * outright. {@link #rewrite} declines several shapes on purpose and its outer catch returns the input untouched;
     * each of those leaves a dotted reference behind, and they must be dropped here rather than pushed.
     * <p>
     * Weakening only: a removed predicate becomes {@code alwaysTrue}, and an {@code OR} or {@code NOT} containing one
     * collapses whole, so the pushed filter can only admit more files. The engine still applies the predicate to the
     * rows, so results are unchanged.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Correctness guard: drops dotted VARIANT sub-field references the rewriter declined, which would otherwise fail scan planning with a ValidationException; mirrors withoutExtractTerms' weaken-only discipline")
    public static Expression withoutVariantSubFieldPredicates(Expression expression, Schema schema) {
        if (expression == null || schema == null) {
            return expression;
        }
        if (expression instanceof And) {
            And and = (And) expression;
            return Expressions.and(withoutVariantSubFieldPredicates(and.left(), schema),
                    withoutVariantSubFieldPredicates(and.right(), schema));
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            return containsVariantSubFieldPredicate(or, schema) ? Expressions.alwaysTrue()
                    : Expressions.or(withoutVariantSubFieldPredicates(or.left(), schema),
                            withoutVariantSubFieldPredicates(or.right(), schema));
        } else if (expression instanceof Not) {
            Not not = (Not) expression;
            return containsVariantSubFieldPredicate(not, schema) ? Expressions.alwaysTrue()
                    : Expressions.not(withoutVariantSubFieldPredicates(not.child(), schema));
        } else if (expression instanceof UnboundPredicate
                && isVariantSubFieldReference((UnboundPredicate<?>) expression, schema)) {
            return Expressions.alwaysTrue();
        }
        return expression;
    }

    /**
     * Returns {@code expression} with every extract predicate replaced by {@code alwaysTrue}, yielding a filter that is
     * <em>weaker than or equal to</em> the original and therefore safe to push to {@code TableScan.filter(..)}.
     * <p>
     * This is needed because Iceberg 1.11.0 cannot put an extract term through a scan at all: {@code SnapshotScan}
     * logs every scan via {@code ExpressionUtil.toSanitizedString}, whose {@code describe(Term)} handles only
     * {@code BoundReference} and {@code BoundTransform} and throws {@code UnsupportedOperationException:
     * Unsupported term: extract(..)} for anything else — for bound and unbound terms alike, and unconditionally
     * (the log argument is evaluated before any level check). Upstream fix: apache/iceberg PR #15384.
     * <p>
     * Weakening rules — a scan filter may only ever admit <em>more</em> files, never fewer:
     * <ul>
     * <li>{@code AND}: strip each side; dropping a conjunct weakens the filter, which is safe.</li>
     * <li>{@code OR}: if either side contains an extract term the whole disjunction becomes {@code alwaysTrue};
     * dropping a disjunct would <em>strengthen</em> the filter and could wrongly skip files.</li>
     * <li>{@code NOT}: if the child contains an extract term the negation becomes {@code alwaysTrue}, since a weaker
     * child yields a stronger negation.</li>
     * </ul>
     * The extract predicates themselves are not lost: the caller applies the full expression to each planned data file
     * with {@link VariantBoundsEvaluator}.
     *
     * @implNote TODO(iceberg-15384): delete once apache/iceberg#15384 ships. The whole reason to weaken the pushed
     *           filter is that {@code SnapshotScan.planFiles()} throws while sanitizing an extract term for its log
     *           line; with the fix the full expression can be pushed, which also restores manifest-level
     *           short-circuiting. https://github.com/apache/iceberg/pull/15384
     */
    public static Expression withoutExtractTerms(Expression expression) {
        if (expression == null) {
            return null;
        }
        if (expression instanceof And) {
            And and = (And) expression;
            return Expressions.and(withoutExtractTerms(and.left()), withoutExtractTerms(and.right()));
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            return containsExtractTerm(or) ? Expressions.alwaysTrue()
                    : Expressions.or(withoutExtractTerms(or.left()), withoutExtractTerms(or.right()));
        } else if (expression instanceof Not) {
            Not not = (Not) expression;
            return containsExtractTerm(not) ? Expressions.alwaysTrue()
                    : Expressions.not(withoutExtractTerms(not.child()));
        } else if (expression instanceof UnboundPredicate
                && ((UnboundPredicate<?>) expression).term() instanceof UnboundExtract) {
            return Expressions.alwaysTrue();
        }
        return expression;
    }

    /**
     * @return the names of the columns referenced by extract terms in {@code expression}. Callers need these to ask
     *         Iceberg for those columns' statistics: planned data files drop column stats unless
     *         {@code Scan#includeColumnStats} requests them, and without bounds an evaluator can never prune.
     *
     * @implNote TODO(iceberg-15384): delete once apache/iceberg#15384 ships — Iceberg evaluates the predicate during
     *           planning and reads the manifest stats itself, so nothing has to request them back on planned files.
     *           https://github.com/apache/iceberg/pull/15384
     */
    public static Set<String> extractTermColumns(Expression expression) {
        Set<String> columns = new LinkedHashSet<>();
        collectExtractColumns(expression, columns);
        return columns;
    }

    private static void collectExtractColumns(Expression expression, Set<String> columns) {
        if (expression instanceof And) {
            And and = (And) expression;
            collectExtractColumns(and.left(), columns);
            collectExtractColumns(and.right(), columns);
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            collectExtractColumns(or.left(), columns);
            collectExtractColumns(or.right(), columns);
        } else if (expression instanceof Not) {
            collectExtractColumns(((Not) expression).child(), columns);
        } else if (expression instanceof UnboundPredicate) {
            org.apache.iceberg.expressions.Term term = ((UnboundPredicate<?>) expression).term();
            if (term instanceof UnboundExtract) {
                columns.add(((UnboundExtract<?>) term).ref().name());
            }
        }
    }

    /**
     * @return {@code false} if any segment contains a character dot-notation cannot express unambiguously, in which
     *         case the path must not be rewritten (see the caller for why).
     *
     * @implNote TODO(iceberg-15384): delete once apache/iceberg#15384 ships. It adds bracket-notation paths
     *           ({@code $['a']['b']}) precisely so names containing dots can be expressed; switch path building to
     *           brackets and this restriction disappears, gaining pushdown for those fields.
     *           https://github.com/apache/iceberg/pull/15384
     */
    private static boolean segmentsExpressibleInDotNotation(List<String> segments) {
        if (segments.isEmpty()) {
            return false;
        }
        for (String segment : segments) {
            if (!RFC9535_MEMBER_NAME_SHORTHAND.matcher(segment).matches()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Converts an extract term's dot path ({@code $.a.b}) into the normalized bracket form Iceberg uses as the
     * <em>key</em> inside a manifest's variant bound object ({@code $['a']['b']}). The two notations differ: the path
     * given to {@code Expressions.extract} must use dots on 1.11.0, while the bound object's keys always use brackets.
     *
     * @implNote TODO(iceberg-15384): delete once apache/iceberg#15384 ships — only {@link VariantBoundsEvaluator}
     *           needs bound keys, because it reads the manifest bounds itself; Iceberg's evaluator resolves them
     *           internally. https://github.com/apache/iceberg/pull/15384
     */
    public static String toBoundKey(String dotPath) {
        if (dotPath == null || !dotPath.startsWith("$")) {
            return null;
        }
        StringBuilder key = new StringBuilder("$");
        for (String segment : splitPath(dotPath.substring(1))) {
            key.append("['").append(segment).append("']");
        }
        return key.toString();
    }

    /** Splits a dotted reference name; exposed for tests and callers needing the same segmentation rules. */
    public static List<String> splitPath(String name) {
        List<String> parts = new ArrayList<>();
        for (String part : name.split("\\.")) {
            if (!part.isEmpty()) {
                parts.add(part);
            }
        }
        return parts;
    }
}
