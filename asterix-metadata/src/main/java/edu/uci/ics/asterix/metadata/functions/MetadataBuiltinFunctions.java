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
package edu.uci.ics.asterix.metadata.functions;

import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class MetadataBuiltinFunctions {

    static {
        addMetadataBuiltinFunctions();
        AsterixBuiltinFunctions.addUnnestFun(AsterixBuiltinFunctions.DATASET, false);
        AsterixBuiltinFunctions.addDatasetFunction(AsterixBuiltinFunctions.DATASET);
        AsterixBuiltinFunctions.addUnnestFun(AsterixBuiltinFunctions.FEED_INGEST, false);
        AsterixBuiltinFunctions.addDatasetFunction(AsterixBuiltinFunctions.FEED_INGEST);
    }

    public static void addMetadataBuiltinFunctions() {

        AsterixBuiltinFunctions.addFunction(AsterixBuiltinFunctions.DATASET, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
                if (f.getArguments().size() != 1) {
                    throw new AlgebricksException("dataset arity is 1, not " + f.getArguments().size());
                }
                ILogicalExpression a1 = f.getArguments().get(0).getValue();
                IAType t1 = (IAType) env.getType(a1);
                if (t1.getTypeTag() == ATypeTag.ANY) {
                    return BuiltinType.ANY;
                }
                if (t1.getTypeTag() != ATypeTag.STRING) {
                    throw new AlgebricksException("Illegal type " + t1 + " for dataset() argument.");
                }
                if (a1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return BuiltinType.ANY;
                }
                AsterixConstantValue acv = (AsterixConstantValue) ((ConstantExpression) a1).getValue();
                String datasetArg = ((AString) acv.getObject()).getStringValue();
                AqlMetadataProvider metadata = ((AqlMetadataProvider) mp);
                Pair<String, String> datasetInfo = getDatasetInfo(metadata, datasetArg);
                String dataverseName = datasetInfo.first;
                String datasetName = datasetInfo.second;
                if (dataverseName == null) {
                    throw new AlgebricksException("Unspecified dataverse!");
                }
                Dataset dataset = metadata.findDataset(dataverseName, datasetName);
                if (dataset == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName + " in dataverse "
                            + dataverseName);
                }
                String tn = dataset.getItemTypeName();
                IAType t2 = metadata.findType(dataverseName, tn);
                if (t2 == null) {
                    throw new AlgebricksException("No type for dataset " + datasetName);
                }
                return t2;
            }
        });

        AsterixBuiltinFunctions.addPrivateFunction(AsterixBuiltinFunctions.FEED_INGEST, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
                if (f.getArguments().size() != 1) {
                    throw new AlgebricksException("dataset arity is 1, not " + f.getArguments().size());
                }
                ILogicalExpression a1 = f.getArguments().get(0).getValue();
                IAType t1 = (IAType) env.getType(a1);
                if (t1.getTypeTag() == ATypeTag.ANY) {
                    return BuiltinType.ANY;
                }
                if (t1.getTypeTag() != ATypeTag.STRING) {
                    throw new AlgebricksException("Illegal type " + t1 + " for dataset() argument.");
                }
                if (a1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return BuiltinType.ANY;
                }
                AsterixConstantValue acv = (AsterixConstantValue) ((ConstantExpression) a1).getValue();
                String datasetArg = ((AString) acv.getObject()).getStringValue();
                AqlMetadataProvider metadata = ((AqlMetadataProvider) mp);
                Pair<String, String> datasetInfo = getDatasetInfo(metadata, datasetArg);
                String dataverseName = datasetInfo.first;
                String datasetName = datasetInfo.second;
                if (dataverseName == null) {
                    throw new AlgebricksException("Unspecified dataverse!");
                }
                Dataset dataset = metadata.findDataset(dataverseName, datasetName);
                if (dataset == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName + " in dataverse "
                            + dataverseName);
                }
                String tn = dataset.getItemTypeName();
                IAType t2 = metadata.findType(dataverseName, tn);
                if (t2 == null) {
                    throw new AlgebricksException("No type for dataset " + datasetName);
                }
                return t2;
            }
        });
    }

    private static Pair<String, String> getDatasetInfo(AqlMetadataProvider metadata, String datasetArg) {
        String[] datasetNameComponents = datasetArg.split("\\.");
        String dataverseName;
        String datasetName;
        if (datasetNameComponents.length == 1) {
            dataverseName = metadata.getDefaultDataverse() == null ? null : metadata.getDefaultDataverse()
                    .getDataverseName();
            datasetName = datasetNameComponents[0];
        } else {
            dataverseName = datasetNameComponents[0];
            datasetName = datasetNameComponents[1];
        }
        return new Pair(dataverseName, datasetName);
    }
}
