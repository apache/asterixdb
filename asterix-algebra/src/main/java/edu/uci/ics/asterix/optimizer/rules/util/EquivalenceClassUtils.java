/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.optimizer.rules.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableObject;
import org.mortbay.util.SingletonList;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

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
            List<LogicalVariable> indexSearchVars, ARecordType recordType, Dataset dataset, IOptimizationContext context)
            throws AlgebricksException {
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
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(recordVar)),
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                            fieldIndexInRecord)))));
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
