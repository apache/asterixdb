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
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.mortbay.util.SingletonList;

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
        LogicalVariable recordVar = hasMeta ? indexSearchVars.get(indexSearchVars.size() - 2)
                : indexSearchVars.get(indexSearchVars.size() - 1);
        LogicalVariable metaRecordVar = hasMeta ? indexSearchVars.get(indexSearchVars.size() - 1) : null;
        for (int pkIndex = 0; pkIndex < primaryKey.size(); ++pkIndex) {
            LogicalVariable referredRecordVar = recordVar;
            String pkFieldName = primaryKey.get(pkIndex).get(0);
            Integer fieldIndexInRecord = fieldNameToIndexMap.get(pkFieldName);
            if (fieldIndexInRecord == null && hasMeta) {
                referredRecordVar = metaRecordVar;
                pkFieldName = primaryKey.get(pkIndex).get(1);
                fieldIndexInRecord = metaFieldNameToIndexMap.get(pkFieldName);
            }
            LogicalVariable var = indexSearchVars.get(pkIndex);
            ILogicalExpression expr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(referredRecordVar)),
                    new MutableObject<ILogicalExpression>(
                            new ConstantExpression(new AsterixConstantValue(new AInt32(fieldIndexInRecord)))));
            EquivalenceClass equivClass = new EquivalenceClass(SingletonList.newSingletonList(var), var,
                    SingletonList.newSingletonList(expr));
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
     * @param context
     *            the optimization context.
     * @param operator
     *            the operator of interest.
     * @return a set of covering variables that can imply all subplan-local live variables at <code>operator</code>.
     * @throws AlgebricksException
     */
    public static Set<LogicalVariable> findFDHeaderVariables(IOptimizationContext context, ILogicalOperator operator)
            throws AlgebricksException {
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses((AbstractLogicalOperator) operator, context);
        List<FunctionalDependency> fds = context.getFDList(operator);
        context.clearAllFDAndEquivalenceClasses();

        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getSubplanLocalLiveVariables(operator, liveVars);

        Set<LogicalVariable> key = new HashSet<>();
        Set<LogicalVariable> cover = new HashSet<>();
        for (FunctionalDependency fd : fds) {
            List<LogicalVariable> head = fd.getHead();
            head.retainAll(liveVars);
            key.addAll(head);
            cover.addAll(fd.getTail());
            if (cover.containsAll(liveVars)) {
                return key;
            }
        }
        if (cover.containsAll(liveVars)) {
            return key;
        } else {
            IVariableTypeEnvironment env = context.getOutputTypeEnvironment(operator);
            Set<LogicalVariable> keyVars = new HashSet<>();
            for (LogicalVariable var : liveVars) {
                IAType type = (IAType) env.getVarType(var);
                ATypeTag typeTag = type.getTypeTag();
                if (typeTag == ATypeTag.RECORD || typeTag == ATypeTag.ORDEREDLIST
                        || typeTag == ATypeTag.UNORDEREDLIST) {
                    continue;
                }
                keyVars.add(var);
            }
            return keyVars;
        }
    }
}
