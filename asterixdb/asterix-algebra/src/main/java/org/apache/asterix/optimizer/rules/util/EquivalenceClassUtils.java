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

package org.apache.asterix.optimizer.rules.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.PrimaryKeyVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;

public class EquivalenceClassUtils {

    /**
     * Adds equivalent classes for primary index accesses, including unnest-map for
     * primary index access and data source scan through primary index ---
     * one equivalent class between a primary key variable and a record field-access expression.
     *
     * @param operator
     *            , the primary index access operator.
     * @param indexSearchVars
     *            , the returned variables from primary index access. The last variable
     *            is the record variable.
     * @param recordType
     *            , the record type of an index payload record.
     * @param metaRecordType
     *            , the type of a meta record associated with an index payload record.
     * @param dataset
     *            , the accessed dataset.
     * @param context
     *            , the optimization context.
     * @throws AlgebricksException
     */
    @SuppressWarnings("unchecked")
    public static void addEquivalenceClassesForPrimaryIndexAccess(ILogicalOperator operator,
            List<LogicalVariable> indexSearchVars, ARecordType recordType, ARecordType metaRecordType, Dataset dataset,
            IOptimizationContext context) throws AlgebricksException {
        if (dataset.getDatasetDetails().getDatasetType() != DatasetType.INTERNAL) {
            return;
        }
        InternalDatasetDetails datasetDetails = (InternalDatasetDetails) dataset.getDatasetDetails();
        List<List<String>> primaryKey = datasetDetails.getPrimaryKey();
        Map<String, Integer> fieldNameToIndexMap = new HashMap<String, Integer>();
        String[] fieldNames = recordType.getFieldNames();
        for (int fieldIndex = 0; fieldIndex < fieldNames.length; ++fieldIndex) {
            fieldNameToIndexMap.put(fieldNames[fieldIndex], fieldIndex);
        }
        boolean hasMeta = dataset.hasMetaPart();
        Map<String, Integer> metaFieldNameToIndexMap = new HashMap<>();
        if (hasMeta) {
            String[] metaFieldNames = metaRecordType.getFieldNames();
            for (int metaFieldIndex = 0; metaFieldIndex < metaFieldNames.length; ++metaFieldIndex) {
                metaFieldNameToIndexMap.put(metaFieldNames[metaFieldIndex], metaFieldIndex);
            }
        }
        List<Integer> keySourceIndicators = datasetDetails.getKeySourceIndicator();
        LogicalVariable recordVar = hasMeta ? indexSearchVars.get(indexSearchVars.size() - 2)
                : indexSearchVars.get(indexSearchVars.size() - 1);
        LogicalVariable metaRecordVar = hasMeta ? indexSearchVars.get(indexSearchVars.size() - 1) : null;
        for (int pkIndex = 0; pkIndex < primaryKey.size(); ++pkIndex) {
            LogicalVariable referredRecordVar = recordVar;
            String pkFieldName = primaryKey.get(pkIndex).get(0);
            int source = keySourceIndicators.get(pkIndex);
            Integer fieldIndexInRecord;
            if (source == 0) {
                // The field is from the main record.
                fieldIndexInRecord = fieldNameToIndexMap.get(pkFieldName);
            } else {
                // The field is from the auxiliary meta record.
                referredRecordVar = metaRecordVar;
                fieldIndexInRecord = metaFieldNameToIndexMap.get(pkFieldName);
            }
            LogicalVariable var = indexSearchVars.get(pkIndex);
            VariableReferenceExpression referredRecordVarRef = new VariableReferenceExpression(referredRecordVar);
            referredRecordVarRef.setSourceLocation(operator.getSourceLocation());
            ScalarFunctionCallExpression expr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                    new MutableObject<ILogicalExpression>(referredRecordVarRef), new MutableObject<ILogicalExpression>(
                            new ConstantExpression(new AsterixConstantValue(new AInt32(fieldIndexInRecord)))));
            expr.setSourceLocation(operator.getSourceLocation());
            EquivalenceClass equivClass =
                    new EquivalenceClass(Collections.singletonList(var), var, Collections.singletonList(expr));
            Map<LogicalVariable, EquivalenceClass> equivalenceMap = context.getEquivalenceClassMap(operator);
            if (equivalenceMap == null) {
                equivalenceMap = new HashMap<LogicalVariable, EquivalenceClass>();
                context.putEquivalenceClassMap(operator, equivalenceMap);
            }
            equivalenceMap.put(var, equivClass);
        }
    }

    /**
     * Find the header variables that can imply all subplan-local live variables at <code>operator</code>.
     *
     * @param operator
     *            the operator of interest.
     * @param usedForCorrelationJoin
     *            whether the generated primary key will be used for a join that recovers the correlation.
     * @param context
     *            the optimization context.
     * @return Pair<ILogicalOperator, Set<LogicalVariable>>, an operator (which is either the original parameter
     *         <code>operator</code> or a newly created operator) and
     *         a set of primary key variables at the operator.
     * @throws AlgebricksException
     */
    public static Pair<ILogicalOperator, Set<LogicalVariable>> findOrCreatePrimaryKeyOpAndVariables(
            ILogicalOperator operator, boolean usedForCorrelationJoin, IOptimizationContext context)
            throws AlgebricksException {
        computePrimaryKeys(operator, context);

        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getSubplanLocalLiveVariables(operator, liveVars);

        Set<LogicalVariable> primaryKeyVars = new HashSet<>();
        Set<LogicalVariable> noKeyVars = new HashSet<>();
        for (LogicalVariable liveVar : liveVars) {
            List<LogicalVariable> keyVars = context.findPrimaryKey(liveVar);
            if (keyVars != null) {
                keyVars.retainAll(liveVars);
            }
            if ((keyVars == null || keyVars.isEmpty())) {
                noKeyVars.add(liveVar);
            } else {
                primaryKeyVars.addAll(keyVars);
            }
        }
        primaryKeyVars.retainAll(liveVars);
        if (primaryKeyVars.containsAll(noKeyVars)) {
            return new Pair<ILogicalOperator, Set<LogicalVariable>>(operator, primaryKeyVars);
        } else {
            LogicalVariable assignVar = context.newVar();
            ILogicalOperator assignOp = new AssignOperator(assignVar,
                    new MutableObject<ILogicalExpression>(new StatefulFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_QUERY_UID), null)));
            OperatorPropertiesUtil.markMovable(assignOp, !usedForCorrelationJoin);
            assignOp.getInputs().add(new MutableObject<ILogicalOperator>(operator));
            context.addPrimaryKey(new FunctionalDependency(Collections.singletonList(assignVar),
                    new ArrayList<LogicalVariable>(liveVars)));
            context.computeAndSetTypeEnvironmentForOperator(assignOp);
            return new Pair<ILogicalOperator, Set<LogicalVariable>>(assignOp, Collections.singleton(assignVar));
        }
    }

    private static void computePrimaryKeys(ILogicalOperator op, IOptimizationContext ctx) throws AlgebricksException {
        PrimaryKeyVariablesVisitor visitor = new PrimaryKeyVariablesVisitor();
        PhysicalOptimizationsUtil.visitOperatorAndItsDescendants(op, visitor, ctx);
    }

}
