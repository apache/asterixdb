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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundExtract;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Decides whether a data file can be skipped for a predicate on a shredded {@code VARIANT} sub-field, by comparing the
 * literal against the sub-field's lower/upper bounds recorded in the manifest.
 * <p>
 * This duplicates what Iceberg's {@code InclusiveMetricsEvaluator} is meant to do, because on Iceberg 1.11.0 that
 * evaluator cannot read variant bounds back out of a manifest: it does not force little-endian and throws
 * {@code IllegalArgumentException: Unsupported byte order: big endian} (fixed upstream by apache/iceberg PR #15384,
 * in review at the time of writing). Bounds read with the correct byte order are perfectly valid, so the data is
 * usable today — only Iceberg's reader of it is broken. When that fix ships, delete this class and hand the extract
 * predicate to {@code TableScan.filter(..)} instead.
 * <p>
 * <b>Safety model.</b> Skipping a file that could contain a matching row silently loses data, so every branch here
 * fails towards keeping the file. A file is dropped only when the bounds <em>prove</em> no row can match. Anything
 * unexpected — bounds absent, buffer unparseable, sub-field missing from the bound object, a type pairing we cannot
 * compare, an operator we do not model, a {@code NOT} — returns "might match".
 * <p>
 * <b>Known semantics inherited from the format.</b> Bounds for a variant sub-field are only collected for values that
 * match the shredded type (see the Iceberg variant-metadata proposal), so a mixed-type sub-field's bounds describe
 * only the type-aligned values. This evaluator therefore only ever compares like with like: if the literal's type does
 * not match the bound's type, it keeps the file rather than guessing.
 *
 * <h2>TODO(iceberg-15384): remove this class and everything below once the Iceberg fix is merged <b>and</b>
 * included</h2>
 *
 * Tracking: <a href="https://github.com/apache/iceberg/pull/15384">apache/iceberg#15384</a> — "Api: Support variant
 * extract and fix manifest bounds byte order" (open and in review at the time of writing). The trigger for removal is
 * that PR being merged upstream <em>and</em> the Iceberg release this build depends on having picked it up (1.11.0 at
 * the time of writing): merged-but-unreleased is not enough, because the defect is still in the jar we compile and run
 * against. The tripwire test named below is what tells you the condition is met — it fails on the upgrade.
 * <p>
 * It fixes <em>all three</em> defects that forced the workarounds:
 * <ol>
 * <li>{@code ExpressionUtil.describe} throwing {@code Unsupported term: extract(..)}, which makes
 * {@code SnapshotScan.planFiles()} fail while logging the filter (so an extract predicate cannot be given to
 * {@code TableScan.filter(..)} at all); and</li>
 * <li>{@code InclusiveMetricsEvaluator.parseBounds} not forcing little-endian, so variant bounds read back from a
 * manifest throw {@code Unsupported byte order: big endian}; and</li>
 * <li>{@code PathUtil.parse} rejecting bracket paths, which leaves a sub-field named with a dot inexpressible — the
 * PR's own review raised exactly this ("use bracket for field names with dot, or other special characters"). Until it
 * ships, {@link VariantPredicateRewriter} declines those paths instead of guessing.</li>
 * </ol>
 *
 * <b>Removal checklist</b> — every site carries the marker {@code iceberg-15384}, so {@code grep -rn "iceberg-15384"}
 * finds them all:
 * <ul>
 * <li>delete this class and its tests ({@code VariantBoundsEvaluatorTest}, {@code VariantBoundsAllTypesTest}) — the
 * type/corner-case coverage should be re-pointed at Iceberg's evaluator rather than deleted outright;</li>
 * <li>in {@code IcebergParquetRecordReaderFactory#configure}: drop the extract/non-extract filter split, the
 * {@code includeColumnStats(..)} call and the per-task evaluation loop, and simply pass the rewritten expression to
 * {@code scan.filter(..)} — that also restores manifest-level short-circuiting, which the local loop cannot do;</li>
 * <li>in {@link VariantPredicateRewriter}: delete {@code containsExtractTerm}, {@code withoutExtractTerms},
 * {@code toBoundKey} and {@code extractTermColumns}, and switch path building from dot notation to the bracket form
 * that #15384 introduces, which also removes the dotted-field-name restriction;</li>
 * <li>the tripwire test {@code scanWorkaround_icebergCannotSanitizeExtract} will start failing on upgrade — that is
 * its purpose; delete it once the workarounds are gone.</li>
 * </ul>
 * Note that {@link VariantSchemaClipper}, {@link RequestedVariantPaths}, {@link VariantProjectionPlan} and the
 * projected Parquet reader are <em>not</em> affected: they implement column projection, which #15384 does not address.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Evaluates shredded-variant sub-field manifest bounds ourselves (little-endian) to skip data files, "
        + "because Iceberg 1.11.0's InclusiveMetricsEvaluator throws on variant bounds; conservative by "
        + "construction — only drops a file when the bounds prove no row can match")
public final class VariantBoundsEvaluator {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Schema schema;
    private final Expression expression;
    private final IWarningCollector warningCollector;

    public VariantBoundsEvaluator(Schema schema, Expression expression, IWarningCollector warningCollector) {
        this.schema = schema;
        this.expression = expression;
        this.warningCollector = warningCollector;
    }

    /**
     * Reports that a sub-field's bounds could not be evaluated, so its files could not be skipped.
     * <p>
     * Raised as a warning and not only logged, because the consequence is invisible otherwise: results stay correct and
     * only the number of files read changes, so bounds that stopped being readable look like the optimization simply not
     * helping. The warning names the sub-field rather than the data file: the sub-field is the actionable part, it is
     * stable across every file in the scan so repeats fold into a single count, and it keeps the message free of a
     * path. The failing file, and the cause, go to the debug log instead - with the location redacted, since a log is
     * not the requester's to read.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Surfaces unevaluable variant bounds as a deduplicated warning naming the sub-field, with the redacted file location and cause on the debug log")
    private void warnBoundsNotEvaluated(String subField) {
        if (warningCollector != null && warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(null, ErrorCode.ICEBERG_VARIANT_BOUNDS_NOT_EVALUATED, subField));
        }
    }

    /**
     * @return {@code true} if {@code file} might contain a row matching the expression (so it must be read), and
     *         {@code false} only when the manifest bounds prove that it cannot.
     */
    public boolean mightMatch(ContentFile<?> file) {
        try {
            return eval(expression, file);
        } catch (RuntimeException e) {
            // A pure backstop: unlike the bound-decoding catch below, NO reachable thrower is known here. Every step of
            // eval is null- and type-guarded, and the one Iceberg call that can throw on an unexpected shape
            // (VariantValue.asObject) is guarded by an explicit type check. It is kept because the alternative is that
            // an unforeseen bug in bounds evaluation fails the query outright, when this is only an optimization —
            // "might match" costs a read. If this ever fires, the log is the only trace, hence logging the cause.
            LOGGER.debug("variant bounds evaluation failed for {}; keeping the file",
                    LogRedactionUtil.userData(file.location()), e);
            warnBoundsNotEvaluated(String.join(", ", VariantPredicateRewriter.extractTermColumns(expression)));
            return true;
        }
    }

    private boolean eval(Expression expr, ContentFile<?> file) {
        if (expr instanceof And) {
            And and = (And) expr;
            // Either side proving "cannot match" is enough to skip the file.
            return eval(and.left(), file) && eval(and.right(), file);
        } else if (expr instanceof Or) {
            Or or = (Or) expr;
            return eval(or.left(), file) || eval(or.right(), file);
        } else if (expr instanceof Not) {
            return true; // not modelled; keep
        } else if (expr instanceof UnboundPredicate) {
            return evalPredicate((UnboundPredicate<?>) expr, file);
        }
        return true; // alwaysTrue/alwaysFalse and bound predicates: not our concern
    }

    private boolean evalPredicate(UnboundPredicate<?> predicate, ContentFile<?> file) {
        if (!(predicate.term() instanceof UnboundExtract)) {
            // Not an extract term, so there are no sub-field bounds to consult and the file is kept. Two shapes reach
            // here: an ordinary column predicate, which Iceberg's own planning already applied; and a variant
            // sub-field predicate the rewrite declined, which was stripped from the pushed filter and so was applied
            // by nobody — the engine evaluates it against the rows instead. Keeping the file is correct for both.
            return true;
        }
        UnboundExtract<?> extract = (UnboundExtract<?>) predicate.term();
        NestedField column = schema.findField(extract.ref().name());
        if (column == null) {
            return true;
        }
        Object literal = predicate.literal() == null ? null : predicate.literal().value();
        if (literal == null) {
            return true;
        }
        String boundKey = VariantPredicateRewriter.toBoundKey(extract.path());
        if (boundKey == null) {
            return true;
        }
        Object lower = boundValue(file.lowerBounds(), column.name(), column.fieldId(), boundKey);
        Object upper = boundValue(file.upperBounds(), column.name(), column.fieldId(), boundKey);
        if (lower == null && upper == null) {
            return true; // no usable bounds for this sub-field (not shredded here, or stats absent)
        }

        switch (predicate.op()) {
            case EQ:
                // cannot match if literal is strictly outside [lower, upper]
                return !(isLess(literal, lower) || isGreater(literal, upper));
            case LT:
                return !isGreaterOrEqual(lower, literal); // all values >= literal -> no row is < literal
            case LT_EQ:
                return !isGreater(lower, literal);
            case GT:
                return !isLessOrEqual(upper, literal);
            case GT_EQ:
                return !isLess(upper, literal);
            case NOT_EQ:
            default:
                // NOT_EQ can only be excluded when every value equals the literal, and nulls make that unsafe to
                // infer from bounds alone. Other operators are not modelled.
                return true;
        }
    }

    /** Reads a sub-field's bound out of the manifest bound map, or {@code null} when unavailable. */
    private Object boundValue(Map<Integer, ByteBuffer> bounds, String columnName, int fieldId, String boundKey) {
        if (bounds == null) {
            return null;
        }
        ByteBuffer buffer = bounds.get(fieldId);
        if (buffer == null) {
            return null;
        }
        try {
            // The manifest hands back a big-endian buffer; the variant encoding is little-endian.
            Variant variant = Variant.from(buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN));
            VariantValue value = variant.value();
            if (value.type() != PhysicalType.OBJECT) {
                // Not a failure: a variant column whose value is a scalar rather than an object has a scalar bound, and
                // there is no sub-field in it to compare against. Checked rather than left to asObject()'s
                // IllegalArgumentException, so an ordinary table shape does not travel the error path and warn.
                return null;
            }
            VariantValue subField = value.asObject().get(boundKey);
            return subField == null ? null : supportedPrimitive(subField);
        } catch (RuntimeException e) {
            // Reached when the bound is not a decodable variant at all. Concretely: VariantUtil raises
            // IllegalArgumentException or UnsupportedOperationException on an unrecognised header or type byte, and a
            // truncated buffer surfaces as IndexOutOfBoundsException or BufferUnderflowException from the buffer reads.
            // The catch stays broad because that last group comes from the JDK and the set is not worth enumerating;
            // being wrong about the type would fail the query, which is the outcome this guard exists to prevent.
            LOGGER.debug("unparseable variant bound for field id {} at {}", fieldId, boundKey, e);
            warnBoundsNotEvaluated(columnName + "." + boundKey);
            return null;
        }
    }

    /**
     * Returns the bound's Java value only for physical types this evaluator can compare soundly, and {@code null}
     * otherwise so the file is kept.
     * <p>
     * The exclusions matter. Temporal values (DATE, TIME, the TIMESTAMP variants) surface as plain {@code Integer} /
     * {@code Long} epoch counts, so they would silently compare "successfully" against an ordinary numeric literal and
     * could prune on a comparison the query engine never intended. BINARY, UUID, NULL, and nested object/array values
     * have no ordering we should assume here.
     */
    private static Object supportedPrimitive(VariantValue value) {
        switch (value.type()) {
            case BOOLEAN_TRUE:
            case BOOLEAN_FALSE:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DECIMAL4:
            case DECIMAL8:
            case DECIMAL16:
            case STRING:
                return value.asPrimitive().get();
            default:
                return null;
        }
    }

    // ---- comparisons: every helper returns false when the pair cannot be compared, so callers keep the file ----

    private static boolean isLess(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c < 0;
    }

    private static boolean isLessOrEqual(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c <= 0;
    }

    private static boolean isGreater(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c > 0;
    }

    private static boolean isGreaterOrEqual(Object a, Object b) {
        Integer c = compare(a, b);
        return c != null && c >= 0;
    }

    /**
     * Compares a bound value with a literal, or returns {@code null} when the pair is not comparable — a differing
     * kind (number vs string), an unsupported type, or a null operand. Returning {@code null} makes every caller
     * behave as "cannot prove anything", so the file is kept.
     */
    private static Integer compare(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (a instanceof Number && b instanceof Number) {
            Number na = (Number) a;
            Number nb = (Number) b;
            if (na instanceof BigDecimal || nb instanceof BigDecimal) {
                // Never go through longValue() here: a DECIMAL16 bound overflows a long, which would produce a
                // nonsense ordering and could drop a file that matches.
                BigDecimal da = toBigDecimal(na);
                BigDecimal db = toBigDecimal(nb);
                return (da == null || db == null) ? null : da.compareTo(db);
            }
            if (isFloatingPoint(na) || isFloatingPoint(nb)) {
                double da = na.doubleValue();
                double db = nb.doubleValue();
                if (Double.isNaN(da) || Double.isNaN(db)) {
                    return null; // NaN has no ordering; refuse to prune
                }
                // IEEE comparison, deliberately NOT Double.compare: that imposes a total order in which -0.0 < 0.0,
                // while the query engine treats them as equal. Iceberg widens a zero bound to [-0.0, 0.0], so the
                // total order would let us prune a file whose value is -0.0 for a "= 0.0" predicate, dropping a row
                // that really matches.
                if (da < db) {
                    return -1;
                }
                return da > db ? 1 : 0;
            }
            return Long.compare(na.longValue(), nb.longValue());
        }
        if (a instanceof CharSequence && b instanceof CharSequence) {
            return a.toString().compareTo(b.toString());
        }
        if (a instanceof Boolean && b instanceof Boolean) {
            return Boolean.compare((Boolean) a, (Boolean) b);
        }
        return null;
    }

    private static boolean isFloatingPoint(Number n) {
        return n instanceof Double || n instanceof Float;
    }

    /** Exact widening to {@link BigDecimal}, or {@code null} for values with no finite decimal value. */
    private static BigDecimal toBigDecimal(Number n) {
        if (n instanceof BigDecimal) {
            return (BigDecimal) n;
        }
        if (n instanceof Double || n instanceof Float) {
            double d = n.doubleValue();
            return (Double.isNaN(d) || Double.isInfinite(d)) ? null : BigDecimal.valueOf(d);
        }
        if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
            return BigDecimal.valueOf(n.longValue());
        }
        return null;
    }
}
