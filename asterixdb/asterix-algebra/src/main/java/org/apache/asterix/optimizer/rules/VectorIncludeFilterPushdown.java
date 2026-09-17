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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Decides whether a {@code WHERE} predicate can be evaluated entirely from a vector index's
 * {@code INCLUDE} columns, and if so rewrites it to read those columns out of the secondary tuple.
 * <p>
 * Two callers share this analysis, and they must never disagree:
 * <ul>
 *   <li>{@link PushFilterIntoVectorSearchRule}, which moves the predicate into the vector search during
 *       {@code physicalRewritesTopLevel}. It does this for both plan shapes.</li>
 *   <li>{@code IntroduceTopKAccessMethodRule} / {@code VectorIndexAccessMethod}, which need the same
 *       answer during the {@code accessMethod} phase to decide whether the index-only plan can carry the
 *       predicate, and to rebind it onto the index's INCLUDE columns. That decision is irreversible —
 *       {@code indexOnly} is serialized into the index-search function arguments before the index-only
 *       branch runs — so a gate that admitted a predicate this analysis then declined would leave the
 *       branch unable to rebind it, with no way back to lookup-and-rerank. Sharing one routine is what
 *       makes the two impossible to drift apart.</li>
 * </ul>
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Extracted from PushFilterIntoVectorSearchRule so the index-only gate and the pushdown "
        + "share one decision")
public final class VectorIncludeFilterPushdown {

    private VectorIncludeFilterPushdown() {
    }

    /**
     * Annotation key for the filter variable to physical field index mapping.
     * Value type: Map&lt;LogicalVariable, Integer&gt;
     */
    public static final String VECTOR_FILTER_VAR_MAPPING = "VECTOR_FILTER_VAR_MAPPING";

    /**
     * Annotation key for the filter variable to type mapping.
     * Value type: Map&lt;LogicalVariable, IAType&gt;
     */
    public static final String VECTOR_FILTER_VAR_TYPES = "VECTOR_FILTER_VAR_TYPES";

    /**
     * Register the INCLUDE columns a rewritten predicate reads as variables produced by the vector index
     * unnest-map, and hand {@link org.apache.asterix.algebra.operators.physical.VectorSearchPOperator} their
     * physical field indexes via the annotations above.
     * <p>
     * This only declares the columns; the predicate itself stays wherever the caller put it. The index-only
     * plan uses that: it binds its predicate to these variables but leaves it in a {@code SELECT} above the
     * unnest-map, so the predicate remains an ordinary expression over ordinary variables for every rule
     * that runs before {@link PushFilterIntoVectorSearchRule} moves it in.
     */
    public static void declareFilterVariables(UnnestMapOperator vectorUnnest, PushedIncludeFilter pushed) {
        vectorUnnest.getAnnotations().put(VECTOR_FILTER_VAR_MAPPING, pushed.varToFieldIndex());
        vectorUnnest.getAnnotations().put(VECTOR_FILTER_VAR_TYPES, pushed.varTypes());

        for (Map.Entry<LogicalVariable, IAType> entry : pushed.varTypes().entrySet()) {
            vectorUnnest.getVariables().add(entry.getKey());
            vectorUnnest.getVariableTypes().add(entry.getValue());
        }
    }

    /**
     * Declare the INCLUDE columns and move the predicate into the unnest-map's select condition, which is
     * where the runtime reads it from. Only {@link PushFilterIntoVectorSearchRule} calls this, during the
     * physical rewrites: a select condition reads variables the operator produces rather than variables from
     * its input, which is not what an expression on an operator normally means, so it is created as late as
     * possible to keep it out of the way of the rules that walk expressions.
     */
    public static void apply(UnnestMapOperator vectorUnnest, PushedIncludeFilter pushed) {
        declareFilterVariables(vectorUnnest, pushed);
        vectorUnnest.setSelectCondition(new MutableObject<>(pushed.condition()));
    }

    /**
     * Whether {@code vectorUnnest} already has its INCLUDE filter variables declared -- i.e. the index-only
     * plan bound a predicate to them during the access-method phase and left it in a SELECT for
     * {@link PushFilterIntoVectorSearchRule} to move in.
     */
    public static boolean hasDeclaredFilterVariables(UnnestMapOperator vectorUnnest) {
        return vectorUnnest.getAnnotations().containsKey(VECTOR_FILTER_VAR_MAPPING);
    }

    /**
     * A predicate that has been fully rewritten against a vector index's INCLUDE columns.
     *
     * @param condition the rewritten predicate, referencing only the variables in {@code varToFieldIndex}
     * @param varToFieldIndex each fresh filter variable's physical field index in the secondary tuple
     * @param varTypes each fresh filter variable's type, for the filter's type environment
     */
    public record PushedIncludeFilter(ILogicalExpression condition, Map<LogicalVariable, Integer> varToFieldIndex,
            Map<LogicalVariable, IAType> varTypes) {
    }

    /**
     * The index-side inputs the analysis needs. Grouped so the two call sites, which obtain them from
     * different places (a metadata lookup keyed off the plan versus the already-chosen index), cannot pass
     * them in a different order.
     *
     * @param includeFieldNames the index's INCLUDE field paths, in index order
     * @param recordType the dataset's record type, used to resolve {@code FIELD_ACCESS_BY_INDEX} and to
     *                   type each filter variable
     * @param isQuantized whether the index is quantized, which sets the secondary-field count
     * @param numPrimaryKeys the dataset's primary key count
     * @param recordVars the variables holding the record(s) this vector search feeds; a field access must
     *                   be rooted at one of these to be a pushdown candidate
     */
    public record IndexContext(List<List<String>> includeFieldNames, ARecordType recordType, boolean isQuantized,
            int numPrimaryKeys, Set<LogicalVariable> recordVars) {
    }

    /**
     * Whether {@code condition} can be pushed, without allocating any variables or touching the
     * optimization context. Used by the cost model and by the index-only gate.
     */
    public static boolean isPushable(ILogicalExpression condition, ILogicalOperator selectOp, IndexContext idx,
            IOptimizationContext context) throws AlgebricksException {
        return analyze(condition, selectOp, idx, context, new ThrowawayVariableSupplier()) != null;
    }

    /**
     * Rewrites {@code condition} to read the index's INCLUDE columns, or returns {@code null} when any part
     * of it cannot be served that way.
     *
     * @param condition the {@code SELECT} condition to push
     * @param selectOp the {@code SELECT} carrying {@code condition}; the {@code ASSIGN} chain below it is
     *                 inlined into the condition so that field accesses on the record are visible
     * @param varSupplier allocates the fresh filter variables — {@code context::newVar} when the result
     *                    will be spliced into the plan, a throwaway supplier when only the verdict matters
     */
    public static PushedIncludeFilter analyze(ILogicalExpression condition, ILogicalOperator selectOp, IndexContext idx,
            IOptimizationContext context, Supplier<LogicalVariable> varSupplier) throws AlgebricksException {
        if (condition == null || idx.includeFieldNames() == null || idx.includeFieldNames().isEmpty()) {
            return null;
        }

        MutableObject<ILogicalExpression> conditionRef = new MutableObject<>(condition.cloneExpression());
        inlineAssigns(conditionRef, selectOp, context);

        // Collect the FULL field paths the filter reads, resolved against the record variable(s) this vector
        // search feeds. Matching on full paths (not leaf names) is what keeps `WHERE m.b.year > 2000` from
        // being pushed against an `INCLUDE (a.year)` column, and restricting the base to this subtree's
        // record variable(s) is what keeps a second record (from a join or a LET) from being redirected into
        // the index's INCLUDE field.
        Set<List<String>> filterFieldPaths = new HashSet<>();
        extractFieldPaths(conditionRef.getValue(), idx, filterFieldPaths);
        if (filterFieldPaths.isEmpty()) {
            return null;
        }

        // Bail out if the filter references any field that is not in the index's INCLUDE list.
        Map<List<String>, Integer> includeFieldIndex = buildIncludeFieldIndex(idx.includeFieldNames());
        // Two identical INCLUDE paths would collapse the map and bind the filter to whichever was inserted
        // last; bail rather than risk pushing a predicate that resolves to the wrong physical field.
        if (includeFieldIndex.size() < idx.includeFieldNames().size()) {
            return null;
        }
        for (List<String> fieldPath : filterFieldPaths) {
            if (!includeFieldIndex.containsKey(fieldPath)) {
                return null;
            }
        }

        // Physical tuple format depends on quantization:
        // Non-quantized: [distance, centroidId, pk_0..pk_{N-1}, include_fields...]
        // Quantized:     [distance, centroidId, qDist, qEmbed, pk_0..pk_{N-1}, include_fields...]
        // INCLUDE fields start after the secondary keys and ALL primary key columns.
        int numSecondaryKeys = idx.isQuantized() ? VTreeDataTupleAccessor.Q_NUM_SECONDARY_FIELDS
                : VTreeDataTupleAccessor.NQ_NUM_SECONDARY_FIELDS;
        int includeFieldPhysicalIndex = numSecondaryKeys + idx.numPrimaryKeys();

        Map<List<String>, LogicalVariable> fieldToNewVar = new HashMap<>();
        // Insertion-ordered (INCLUDE order): the caller appends these to the unnest-map's variable list,
        // so a hash order would make the emitted plan differ run to run for a multi-field predicate.
        Map<LogicalVariable, Integer> filterVarToFieldIndex = new LinkedHashMap<>();
        Map<LogicalVariable, IAType> filterVarToType = new LinkedHashMap<>();

        for (List<String> fieldPath : idx.includeFieldNames()) {
            if (filterFieldPaths.contains(fieldPath)) {
                LogicalVariable newVar = varSupplier.get();
                fieldToNewVar.put(fieldPath, newVar);
                filterVarToFieldIndex.put(newVar, includeFieldPhysicalIndex);

                // For open-schema fields not in the type declaration, getSubFieldType returns null;
                // default to ANY so the type environment can still resolve the variable type.
                IAType fieldType = idx.recordType().getSubFieldType(fieldPath);
                if (fieldType == null) {
                    fieldType = BuiltinType.ANY;
                }
                filterVarToType.put(newVar, fieldType);
            }
            includeFieldPhysicalIndex++;
        }

        ILogicalExpression rewritten = rewriteFieldAccess(conditionRef.getValue(), fieldToNewVar, idx);

        // Completeness guard: the pushed condition must reference ONLY the freshly-created INCLUDE
        // variables. The earlier checks act on field paths that resolveFieldPath recognizes; an access
        // pattern it does not recognize, or a non-inlinable ASSIGN output, can leave a reference to the
        // source record in the condition. Embedding such a condition would reference a variable that is not
        // in scope at the unnest -- an invalid plan. Decline instead of emitting one.
        Set<LogicalVariable> rewrittenUsed = new HashSet<>();
        rewritten.getUsedVariables(rewrittenUsed);
        if (!new HashSet<>(fieldToNewVar.values()).containsAll(rewrittenUsed)) {
            return null;
        }

        return new PushedIncludeFilter(rewritten, filterVarToFieldIndex, filterVarToType);
    }

    /**
     * Inline the {@code ASSIGN} chain below the {@code SELECT} into the condition, so that filter references
     * hit field-access expressions on the source record directly rather than an intermediate variable.
     */
    private static void inlineAssigns(MutableObject<ILogicalExpression> conditionRef, ILogicalOperator selectOp,
            IOptimizationContext context) throws AlgebricksException {
        if (selectOp == null || selectOp.getInputs().isEmpty()) {
            return;
        }
        Set<LogicalVariable> usedVariables = new HashSet<>();
        conditionRef.getValue().getUsedVariables(usedVariables);

        InlineVariablesRule.InlineVariablesVisitor inlineVisitor = null;
        Map<LogicalVariable, ILogicalExpression> varAssignRhs = new HashMap<>();

        for (ILogicalOperator child = selectOp.getInputs().get(0).getValue(); child
                .getOperatorTag() == LogicalOperatorTag.ASSIGN; child = child.getInputs().get(0).getValue()) {
            varAssignRhs.clear();
            extractInlinableVariables((AssignOperator) child, usedVariables, varAssignRhs);

            if (!varAssignRhs.isEmpty()) {
                if (inlineVisitor == null) {
                    inlineVisitor = new InlineVariablesRule.InlineVariablesVisitor(varAssignRhs, null);
                    inlineVisitor.setContext(context);
                    // The visitor checks rhs variables against the live-in of this operator; without it the
                    // live-variable lookup has no operator to ask.
                    inlineVisitor.setOperator(selectOp);
                }
                if (!inlineVisitor.transform(conditionRef)) {
                    break;
                }
                usedVariables.clear();
                conditionRef.getValue().getUsedVariables(usedVariables);
            }
            if (child.getInputs().isEmpty()) {
                break;
            }
        }
    }

    private static void extractInlinableVariables(AssignOperator assignOp, Set<LogicalVariable> targetVars,
            Map<LogicalVariable, ILogicalExpression> outMap) {
        List<LogicalVariable> vars = assignOp.getVariables();
        List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();

        for (int i = 0; i < vars.size(); i++) {
            LogicalVariable var = vars.get(i);
            if (targetVars.contains(var)) {
                ILogicalExpression expr = exprs.get(i).getValue();
                if (expr.isFunctional()) {
                    outMap.put(var, expr);
                }
            }
        }
    }

    /**
     * Builds a map from an INCLUDE field's full path to its index in the INCLUDE list.
     * <p>
     * Keyed by the whole path, not the leaf name: {@code INCLUDE (a.year)} must not match a filter on
     * {@code b.year}. {@link List} equality gives exactly path equality.
     */
    private static Map<List<String>, Integer> buildIncludeFieldIndex(List<List<String>> includeFieldNames) {
        Map<List<String>, Integer> result = new HashMap<>();
        for (int i = 0; i < includeFieldNames.size(); i++) {
            result.put(includeFieldNames.get(i), i);
        }
        return result;
    }

    /**
     * Collects the full field paths the filter reads from this vector search's record variable(s).
     * <p>
     * Only accesses rooted at one of {@code idx.recordVars()} are collected. An access on any other record
     * is deliberately ignored here, so it survives the rewrite and trips the completeness guard in
     * {@link #analyze} — which then declines instead of silently evaluating someone else's predicate
     * against an INCLUDE column.
     */
    private static void extractFieldPaths(ILogicalExpression expr, IndexContext idx, Set<List<String>> fieldPaths)
            throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        List<String> path = resolveFieldPath(funcExpr, idx);
        if (path != null) {
            fieldPaths.add(path);
            // Do not recurse: the nested accesses under this one are the prefix of the path just added.
            return;
        }

        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            extractFieldPaths(arg.getValue(), idx, fieldPaths);
        }
    }

    /**
     * Resolve a (possibly nested) field-access expression to its full path relative to one of this vector
     * search's record variables, or {@code null} if it is not such an access.
     * <p>
     * {@code FIELD_ACCESS_BY_INDEX}'s integer is an index into the field names of the type of ITS OWN base
     * expression, so for a nested access the base's type has to be resolved first (descending the record
     * type as the recursion descends) — resolving every level against the top-level record type produces a
     * bogus name, which in the worst case collides with a real INCLUDE path. Both forms are handled because
     * this runs in two phases: before {@code ByNameToByIndexFieldAccessRule} (the index-only gate) accesses
     * are by name, after it (the physical pushdown) a declared type's accesses are by index.
     */
    private static List<String> resolveFieldPath(AbstractFunctionCallExpression funcExpr, IndexContext idx)
            throws AlgebricksException {
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        boolean byName = fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME);
        boolean byIndex = fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX);
        if ((!byName && !byIndex) || funcExpr.getArguments().size() < 2) {
            return null;
        }

        ILogicalExpression base = funcExpr.getArguments().get(0).getValue();
        List<String> prefix;
        if (base.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            LogicalVariable baseVar = ((VariableReferenceExpression) base).getVariableReference();
            if (!idx.recordVars().contains(baseVar)) {
                return null;
            }
            prefix = List.of();
        } else if (base.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            prefix = resolveFieldPath((AbstractFunctionCallExpression) base, idx);
            if (prefix == null) {
                return null;
            }
        } else {
            return null;
        }

        String fieldName;
        if (byName) {
            fieldName = AccessMethodUtils.getStringConstant(funcExpr.getArguments().get(1));
        } else {
            Integer fieldIdx = AccessMethodUtils.getInt32Constant(funcExpr.getArguments().get(1));
            ARecordType baseType = resolveRecordTypeAt(idx.recordType(), prefix);
            if (fieldIdx == null || baseType == null) {
                return null;
            }
            String[] names = baseType.getFieldNames();
            fieldName = fieldIdx >= 0 && fieldIdx < names.length ? names[fieldIdx] : null;
        }
        if (fieldName == null) {
            return null;
        }

        List<String> path = new ArrayList<>(prefix);
        path.add(fieldName);
        return path;
    }

    /**
     * The record type reached by following {@code path} from {@code recordType}, or {@code null} if the path
     * does not resolve to a record type (open field, non-record type, unknown path).
     */
    private static ARecordType resolveRecordTypeAt(ARecordType recordType, List<String> path)
            throws AlgebricksException {
        if (recordType == null) {
            return null;
        }
        if (path.isEmpty()) {
            return recordType;
        }
        IAType type = recordType.getSubFieldType(path);
        if (type != null && type.getTypeTag() == ATypeTag.OBJECT) {
            return (ARecordType) type;
        }
        return null;
    }

    /**
     * Rewrites field-access expressions to use the new INCLUDE field variables.
     * Example: {@code gt($row.getField(2), 2000)} -> {@code gt($year, 2000)}
     */
    private static ILogicalExpression rewriteFieldAccess(ILogicalExpression expr,
            Map<List<String>, LogicalVariable> fieldToVar, IndexContext idx) throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        // Replace a field access with a variable reference only when its FULL path — resolved against this
        // search's record variable — is one of the INCLUDE fields we created a variable for.
        List<String> path = resolveFieldPath(funcExpr, idx);
        if (path != null && fieldToVar.containsKey(path)) {
            LogicalVariable newVar = fieldToVar.get(path);
            VariableReferenceExpression varRef = new VariableReferenceExpression(newVar);
            varRef.setSourceLocation(funcExpr.getSourceLocation());
            return varRef;
        }

        List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>();
        boolean changed = false;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            ILogicalExpression newArg = rewriteFieldAccess(argRef.getValue(), fieldToVar, idx);
            newArgs.add(new MutableObject<>(newArg));
            if (newArg != argRef.getValue()) {
                changed = true;
            }
        }

        if (changed) {
            ScalarFunctionCallExpression newFunc =
                    new ScalarFunctionCallExpression(funcExpr.getFunctionInfo(), newArgs);
            newFunc.setSourceLocation(funcExpr.getSourceLocation());
            return newFunc;
        }

        return expr;
    }

    /**
     * Hands out variables that are never spliced into a plan, for the callers that only need the verdict.
     * Negative ids keep them distinguishable from real ones if they ever leak into a log line.
     */
    private static final class ThrowawayVariableSupplier implements Supplier<LogicalVariable> {
        private int nextId = -1;

        @Override
        public LogicalVariable get() {
            return new LogicalVariable(nextId--);
        }
    }
}
