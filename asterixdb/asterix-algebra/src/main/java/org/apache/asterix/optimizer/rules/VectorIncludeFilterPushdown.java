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
public final class VectorIncludeFilterPushdown {

    private VectorIncludeFilterPushdown() {
    }

    /**
     * Annotation key for the INCLUDE columns a vector index search emits.
     * Value type: {@link IncludeColumns}
     */
    public static final String VECTOR_INCLUDE_COLUMNS = "VECTOR_INCLUDE_COLUMNS";

    /**
     * Every INCLUDE column of the searched index, bound to a variable the search emits.
     * <p>
     * All of them are declared, whether this query reads them or not, so that one mapping serves every use:
     * a predicate pushed into the search, a projection returning the column, or neither. Keyed by the full
     * path, so a nested INCLUDE column ({@code INCLUDE (info.year)}) is addressed exactly like a top-level
     * one and never confused with a same-named field elsewhere in the record.
     *
     * @param pathToVar each column's full path to the variable carrying it, in INCLUDE order; empty when
     *                  the index declares the same path twice, which leaves no unambiguous binding
     * @param varToFieldIndex each variable's physical field index in the secondary tuple
     * @param varTypes each variable's type, for the filter's type environment
     */
    public record IncludeColumns(Map<List<String>, LogicalVariable> pathToVar,
            Map<LogicalVariable, Integer> varToFieldIndex, Map<LogicalVariable, IAType> varTypes) {
    }

    /**
     * Declare every INCLUDE column of the index as an output of the vector index search, appended after the
     * primary keys and, on the index-only plan, the distance.
     * <p>
     * The runtime emits exactly what is declared here, so the columns are bound once, at the point the
     * unnest-map is created, rather than discovered per use. A predicate and a projection over the same
     * column then resolve to the same variable by construction.
     */
    public static IncludeColumns declareIncludeColumns(UnnestMapOperator vectorUnnest, IndexContext idx,
            Supplier<LogicalVariable> varSupplier) throws AlgebricksException {
        IncludeColumns columns = buildIncludeColumns(idx, varSupplier);
        if (columns == null) {
            return null;
        }
        for (Map.Entry<LogicalVariable, IAType> entry : columns.varTypes().entrySet()) {
            vectorUnnest.getVariables().add(entry.getKey());
            vectorUnnest.getVariableTypes().add(entry.getValue());
        }
        vectorUnnest.getAnnotations().put(VECTOR_INCLUDE_COLUMNS, columns);
        return columns;
    }

    /**
     * The index's INCLUDE columns bound to variables from {@code varSupplier}, or {@code null} when the
     * index has none. Touches no operator, so the gate can run the analysis on throwaway variables to reach
     * a verdict before any of this is spliced into a plan.
     */
    private static IncludeColumns buildIncludeColumns(IndexContext idx, Supplier<LogicalVariable> varSupplier)
            throws AlgebricksException {
        List<List<String>> includeFieldNames = idx.includeFieldNames();
        if (includeFieldNames == null || includeFieldNames.isEmpty()) {
            return null;
        }
        Map<List<String>, LogicalVariable> pathToVar = new LinkedHashMap<>();
        Map<LogicalVariable, Integer> varToFieldIndex = new LinkedHashMap<>();
        Map<LogicalVariable, IAType> varTypes = new LinkedHashMap<>();

        int numSecondaryKeys = idx.isQuantized() ? VTreeDataTupleAccessor.Q_NUM_SECONDARY_FIELDS
                : VTreeDataTupleAccessor.NQ_NUM_SECONDARY_FIELDS;
        int fieldIndex = numSecondaryKeys + idx.numPrimaryKeys();
        for (List<String> fieldPath : includeFieldNames) {
            LogicalVariable var = varSupplier.get();
            // An open field not in the type declaration has no declared type; ANY still lets the type
            // environment resolve the variable.
            IAType fieldType = idx.recordType().getSubFieldType(fieldPath);
            if (fieldType == null) {
                fieldType = BuiltinType.ANY;
            }
            pathToVar.put(fieldPath, var);
            varToFieldIndex.put(var, fieldIndex++);
            varTypes.put(var, fieldType);
        }
        // Every column is still declared -- the tuple the runtime writes has to match the declaration either
        // way -- but two identical INCLUDE paths leave no unambiguous variable to bind a reference to, so
        // nothing is bound and the predicate stays above the search. DDL rejects such an index
        // (INDEX_ILLEGAL_REPETITIVE_FIELD), so this is a belt-and-braces check, not a reachable path.
        if (pathToVar.size() < includeFieldNames.size()) {
            pathToVar.clear();
        }
        return new IncludeColumns(pathToVar, varToFieldIndex, varTypes);
    }

    /** The INCLUDE columns declared on {@code vectorUnnest}, or {@code null} if it has none. */
    public static IncludeColumns getIncludeColumns(UnnestMapOperator vectorUnnest) {
        return (IncludeColumns) vectorUnnest.getAnnotations().get(VECTOR_INCLUDE_COLUMNS);
    }

    /**
     * Rewrite {@code condition} to read the declared INCLUDE columns instead of the record, or return
     * {@code null} when any part of it cannot be served that way.
     *
     * @param selectOp the {@code SELECT} carrying the condition; the {@code ASSIGN} chain below it is inlined
     *                 so that field accesses on the record are visible
     */
    public static ILogicalExpression bindPredicate(ILogicalExpression condition, ILogicalOperator selectOp,
            IndexContext idx, IOptimizationContext context, IncludeColumns columns) throws AlgebricksException {
        if (condition == null) {
            return null;
        }
        MutableObject<ILogicalExpression> conditionRef = new MutableObject<>(condition.cloneExpression());
        inlineAssigns(conditionRef, selectOp, context);

        // Bind every field access whose path, or whose longest prefix, is an INCLUDE column: `m.info.year`
        // over INCLUDE (info) becomes a field access on the emitted column, exactly as it does in a
        // projection. There is no separate "is every path covered" test; the guard below is the whole
        // decision. A predicate reading no field at all still binds, trivially, to nothing. It has to:
        // leaving it in a SELECT above the search would filter a candidate set the search had already capped.
        Map<List<String>, LogicalVariable> pathToVar = columns == null ? Map.of() : columns.pathToVar();
        ILogicalExpression rewritten = rewriteFieldAccess(conditionRef.getValue(), pathToVar, idx, Map.of());

        // Completeness guard: the pushed condition must reference ONLY INCLUDE column variables. A field
        // access no INCLUDE column covers, an access pattern resolveFieldPath does not recognize, or a
        // non-inlinable ASSIGN output each leave a reference to the source record behind -- a variable that
        // is not in scope at the unnest.
        Set<LogicalVariable> rewrittenUsed = new HashSet<>();
        rewritten.getUsedVariables(rewrittenUsed);
        if (!new HashSet<>(pathToVar.values()).containsAll(rewrittenUsed)) {
            return null;
        }
        return rewritten;
    }

    /**
     * Replace, in place, every field access under {@code exprRef} whose full path is a key of
     * {@code pathToVar} with a reference to that variable.
     * <p>
     * The same rewrite {@link #bindPredicate} applies, without its guard: a projection legitimately reads
     * other things too, which are left alone. An access whose own path does not match is descended into, so
     * a query reading {@code m.info.year} still binds when the column is {@code INCLUDE (info)}.
     * <p>
     * Paths, not names, so this serves a nested column exactly as it serves a top-level one -- and it is the
     * same operation whether the variable carries an INCLUDE column or a primary key.
     *
     * @param bindings the plan's ASSIGN bindings, through which an access split across ASSIGNs
     *                 ({@code $$a := m.info; $$b := $$a.year}) resolves to its full path; see
     *                 {@link #resolveRecordFieldPath(AbstractFunctionCallExpression, IndexContext, Map)}
     */
    public static void bindPaths(Mutable<ILogicalExpression> exprRef, IndexContext idx,
            Map<List<String>, LogicalVariable> pathToVar, Map<LogicalVariable, ILogicalExpression> bindings)
            throws AlgebricksException {
        if (pathToVar == null || pathToVar.isEmpty()) {
            return;
        }
        exprRef.setValue(rewriteFieldAccess(exprRef.getValue(), pathToVar, idx, bindings));
    }

    /**
     * Move the predicate into the unnest-map's select condition, which is where the runtime reads it from.
     * Only {@link PushFilterIntoVectorSearchRule} calls this, during the physical rewrites: a select
     * condition reads variables the operator produces rather than variables from its input, which is not what
     * an expression on an operator normally means, so it is created as late as possible to keep it out of the
     * way of the rules that walk expressions.
     */
    public static void apply(UnnestMapOperator vectorUnnest, ILogicalExpression condition) {
        vectorUnnest.setSelectCondition(new MutableObject<>(condition));
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
     * Whether {@code condition} can be served entirely from the index's INCLUDE columns. Used by the
     * index-only gate, which has to decide before any of those columns exist.
     * <p>
     * A dry run of {@link #bindPredicate} against throwaway variables, rather than a second implementation
     * of the same checks. The gate's answer is irreversible -- {@code indexOnly} is serialized into the
     * index-search arguments before the binding runs, so a gate that admits a predicate the binding then
     * declines leaves the plan unable to rebind it -- which is exactly why the two must not be able to
     * drift apart.
     */
    public static boolean isPushable(ILogicalExpression condition, ILogicalOperator selectOp, IndexContext idx,
            IOptimizationContext context) throws AlgebricksException {
        return bindPredicate(condition, selectOp, idx, context,
                buildIncludeColumns(idx, new ThrowawayVariableSupplier())) != null;
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
     * The full field path {@code funcExpr} reads off one of {@code idx}'s record variables, or {@code null}
     * if it is not a field access on one of them.
     * <p>
     * Full paths throughout: a nested column is addressed as {@code [info, year]} and never matches a
     * top-level {@code year}, and both access forms resolve, so callers do not have to know whether
     * {@code ByNameToByIndexFieldAccessRule} has run.
     */
    public static List<String> resolveRecordFieldPath(AbstractFunctionCallExpression funcExpr, IndexContext idx)
            throws AlgebricksException {
        return resolveFieldPath(funcExpr, idx, Map.of());
    }

    /**
     * As {@link #resolveRecordFieldPath(AbstractFunctionCallExpression, IndexContext)}, also following an
     * access whose base is a variable that {@code bindings} defines as a field access itself.
     * <p>
     * The compiler routinely splits a nested access across ASSIGNs -- {@code $$a := m.info} then
     * {@code $$b := $$a.year} -- so read in place, the outer access has a variable for its base and no path
     * at all, while the inner one reads a column prefix that is not itself a column. Followed through the
     * binding, the two are one access to {@code [info, year]}. The predicate side sidesteps this by inlining
     * the chain first; the projection side cannot rewrite the plan to look, so it resolves through the
     * bindings instead, and the index-only rewrite must do the same or it would leave the outer access
     * unbound and let the neutralized chain collapse the value to MISSING.
     *
     * @param bindings ASSIGN bindings of the plan, variable to defining expression; may be empty
     */
    public static List<String> resolveRecordFieldPath(AbstractFunctionCallExpression funcExpr, IndexContext idx,
            Map<LogicalVariable, ILogicalExpression> bindings) throws AlgebricksException {
        return resolveFieldPath(funcExpr, idx, bindings);
    }

    /** The {@code variable -> expression} bindings of every ASSIGN under {@code root}, {@code root} included. */
    public static Map<LogicalVariable, ILogicalExpression> collectAssignBindings(ILogicalOperator root) {
        Map<LogicalVariable, ILogicalExpression> bindings = new HashMap<>();
        collectAssignBindings(root, bindings);
        return bindings;
    }

    private static void collectAssignBindings(ILogicalOperator op, Map<LogicalVariable, ILogicalExpression> bindings) {
        if (op == null) {
            return;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign = (AssignOperator) op;
            List<LogicalVariable> vars = assign.getVariables();
            List<Mutable<ILogicalExpression>> exprs = assign.getExpressions();
            for (int i = 0; i < vars.size(); i++) {
                bindings.put(vars.get(i), exprs.get(i).getValue());
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            collectAssignBindings(input.getValue(), bindings);
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
    private static List<String> resolveFieldPath(AbstractFunctionCallExpression funcExpr, IndexContext idx,
            Map<LogicalVariable, ILogicalExpression> bindings) throws AlgebricksException {
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
            if (idx.recordVars().contains(baseVar)) {
                prefix = List.of();
            } else {
                // A variable an ASSIGN defines as a field access is that access: the chain the compiler split
                // is followed back to the record. Anything else is a base this search knows nothing about.
                ILogicalExpression bound = bindings.get(baseVar);
                if (bound == null || bound.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    return null;
                }
                prefix = resolveFieldPath((AbstractFunctionCallExpression) bound, idx, bindings);
                if (prefix == null) {
                    return null;
                }
            }
        } else if (base.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            prefix = resolveFieldPath((AbstractFunctionCallExpression) base, idx, bindings);
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
     * Rewrites field-access expressions to use the INCLUDE column variables. An access whose own path is not
     * a column is descended into, so the longest covered prefix binds.
     * Example: {@code gt($row.getField(2), 2000)} -> {@code gt($year, 2000)};
     * {@code gt($row.info.year, 2000)} over {@code INCLUDE (info)} -> {@code gt($info.year, 2000)}
     */
    private static ILogicalExpression rewriteFieldAccess(ILogicalExpression expr,
            Map<List<String>, LogicalVariable> fieldToVar, IndexContext idx,
            Map<LogicalVariable, ILogicalExpression> bindings) throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        // Replace a field access with a variable reference only when its FULL path — resolved against this
        // search's record variable — is one of the INCLUDE fields we created a variable for.
        List<String> path = resolveFieldPath(funcExpr, idx, bindings);
        if (path != null && fieldToVar.containsKey(path)) {
            LogicalVariable newVar = fieldToVar.get(path);
            VariableReferenceExpression varRef = new VariableReferenceExpression(newVar);
            varRef.setSourceLocation(funcExpr.getSourceLocation());
            return varRef;
        }

        List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>();
        boolean changed = false;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            ILogicalExpression newArg = rewriteFieldAccess(argRef.getValue(), fieldToVar, idx, bindings);
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
