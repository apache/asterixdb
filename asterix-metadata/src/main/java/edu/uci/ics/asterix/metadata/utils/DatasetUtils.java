package edu.uci.ics.asterix.metadata.utils;

import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledInternalDatasetDetails;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class DatasetUtils {
    public static IBinaryComparatorFactory[] computeKeysBinaryComparatorFactories(
            AqlCompiledDatasetDecl compiledDatasetDecl, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
            throws AlgebricksException {
        if (compiledDatasetDecl.getDatasetType() == DatasetType.EXTERNAL)
            throw new AlgebricksException("not implemented");
        List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions;
        partitioningFunctions = getPartitioningFunctions(compiledDatasetDecl);
        int numKeys = partitioningFunctions.size();
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[numKeys];
        for (int i = 0; i < numKeys; i++) {
            Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            IAType keyType = evalFactoryAndType.third;
            bcfs[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        return bcfs;
    }

    public static IBinaryHashFunctionFactory[] computeKeysBinaryHashFunFactories(
            AqlCompiledDatasetDecl compiledDatasetDecl, IBinaryHashFunctionFactoryProvider hashFunProvider)
            throws AlgebricksException {
        if (compiledDatasetDecl.getDatasetType() == DatasetType.EXTERNAL)
            throw new AlgebricksException("not implemented");
        List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions;
        partitioningFunctions = ((AqlCompiledInternalDatasetDetails) compiledDatasetDecl.getAqlCompiledDatasetDetails())
                .getPartitioningFunctions();
        int numKeys = partitioningFunctions.size();
        IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[numKeys];
        for (int i = 0; i < numKeys; i++) {
            Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            IAType keyType = evalFactoryAndType.third;
            bhffs[i] = hashFunProvider.getBinaryHashFunctionFactory(keyType);
        }
        return bhffs;
    }

    public static ITypeTraits[] computeTupleTypeTraits(AqlCompiledDatasetDecl compiledDatasetDecl,
            AqlCompiledMetadataDeclarations datasetDecls) throws AlgebricksException {
        if (compiledDatasetDecl.getDatasetType() == DatasetType.EXTERNAL)
            throw new AlgebricksException("not implemented");
        List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions;
        partitioningFunctions = ((AqlCompiledInternalDatasetDetails) compiledDatasetDecl.getAqlCompiledDatasetDetails())
                .getPartitioningFunctions();
        int numKeys = partitioningFunctions.size();
        ITypeTraits[] typeTraits = new ITypeTraits[numKeys + 1];
        for (int i = 0; i < numKeys; i++) {
            Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType = partitioningFunctions
                    .get(i);
            IAType keyType = evalFactoryAndType.third;
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }
        IAType payloadType = datasetDecls.findType(compiledDatasetDecl.getItemTypeName());
        typeTraits[numKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(payloadType);
        return typeTraits;
    }

    public static List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> getPartitioningFunctions(
            AqlCompiledDatasetDecl decl) {
        return ((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails()).getPartitioningFunctions();
    }

    public static String getNodegroupName(AqlCompiledDatasetDecl decl) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails())).getNodegroupName();
    }

    public static AqlCompiledIndexDecl getPrimaryIndex(AqlCompiledDatasetDecl decl) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails())).getPrimaryIndex();

    }

    public static AqlCompiledIndexDecl findSecondaryIndexByName(AqlCompiledDatasetDecl decl, String indexName) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails())
                .findSecondaryIndexByName(indexName));
    }

    public static List<AqlCompiledIndexDecl> findSecondaryIndexesByOneOfTheKeys(AqlCompiledDatasetDecl decl,
            String fieldExpr) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails()))
                .findSecondaryIndexesByOneOfTheKeys(fieldExpr);
    }

    public static int getPositionOfPartitioningKeyField(AqlCompiledDatasetDecl decl, String fieldExpr) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails()))
                .getPositionOfPartitioningKeyField(fieldExpr);
    }

    public static List<String> getPartitioningExpressions(AqlCompiledDatasetDecl decl) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails())).getPartitioningExprs();
    }

    public static List<AqlCompiledIndexDecl> getSecondaryIndexes(AqlCompiledDatasetDecl decl) {
        return (((AqlCompiledInternalDatasetDetails) decl.getAqlCompiledDatasetDetails())).getSecondaryIndexes();
    }

}
