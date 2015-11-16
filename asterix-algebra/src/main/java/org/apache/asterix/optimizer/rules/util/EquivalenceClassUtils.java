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
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
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
     * @param dataset
     *            , the accessed dataset.
     * @param context
     *            , the optimization context.
     * @throws AlgebricksException
     */
    @SuppressWarnings("unchecked")
    public static void addEquivalenceClassesForPrimaryIndexAccess(ILogicalOperator operator,
            List<LogicalVariable> indexSearchVars, ARecordType recordType, Dataset dataset,
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

        LogicalVariable recordVar = indexSearchVars.get(indexSearchVars.size() - 1);
        for (int pkIndex = 0; pkIndex < primaryKey.size(); ++pkIndex) {
            String pkFieldName = primaryKey.get(pkIndex).get(0);
            int fieldIndexInRecord = fieldNameToIndexMap.get(pkFieldName);
            LogicalVariable var = indexSearchVars.get(pkIndex);
            ILogicalExpression expr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(recordVar)),
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

}
