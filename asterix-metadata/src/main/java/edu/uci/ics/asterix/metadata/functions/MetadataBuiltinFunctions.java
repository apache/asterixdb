package edu.uci.ics.asterix.metadata.functions;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
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

        AsterixBuiltinFunctions.add(AsterixBuiltinFunctions.DATASET, new IResultTypeComputer() {

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
                String datasetName = ((AString) acv.getObject()).getStringValue();
                AqlCompiledMetadataDeclarations metadata = ((AqlMetadataProvider) mp).getMetadataDeclarations();
                AqlCompiledDatasetDecl acdd = metadata.findDataset(datasetName);
                if (acdd == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName);
                }
                String tn = acdd.getItemTypeName();
                IAType t2 = metadata.findType(tn);
                if (t2 == null) {
                    throw new AlgebricksException("No type for dataset " + datasetName);
                }
                return t2;
            }
        });

        AsterixBuiltinFunctions.add(AsterixBuiltinFunctions.FEED_INGEST, new IResultTypeComputer() {

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
                String datasetName = ((AString) acv.getObject()).getStringValue();
                AqlCompiledMetadataDeclarations metadata = ((AqlMetadataProvider) mp).getMetadataDeclarations();
                AqlCompiledDatasetDecl acdd = metadata.findDataset(datasetName);
                if (acdd == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName);
                }
                String tn = acdd.getItemTypeName();
                IAType t2 = metadata.findType(tn);
                if (t2 == null) {
                    throw new AlgebricksException("No type for dataset " + datasetName);
                }
                return t2;
            }
        });
    }
}
