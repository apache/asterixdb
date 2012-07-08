package edu.uci.ics.asterix.metadata.utils;

import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class DatasetUtils {
    public static IBinaryComparatorFactory[] computeKeysBinaryComparatorFactories(Dataset dataset,
            ARecordType itemType, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<String> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); i++) {
            IAType keyType = itemType.getFieldType(partitioningKeys.get(i));
            bcfs[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        return bcfs;
    }

    public static IBinaryHashFunctionFactory[] computeKeysBinaryHashFunFactories(Dataset dataset, ARecordType itemType,
            IBinaryHashFunctionFactoryProvider hashFunProvider) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<String> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); i++) {
            IAType keyType = itemType.getFieldType(partitioningKeys.get(i));
            bhffs[i] = hashFunProvider.getBinaryHashFunctionFactory(keyType);
        }
        return bhffs;
    }

    public static ITypeTraits[] computeTupleTypeTraits(Dataset dataset, ARecordType itemType)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();
        ITypeTraits[] typeTraits = new ITypeTraits[numKeys + 1];
        for (int i = 0; i < numKeys; i++) {
            IAType keyType = itemType.getFieldType(partitioningKeys.get(i));
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }
        typeTraits[numKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        return typeTraits;
    }

    public static List<String> getPartitioningKeys(Dataset dataset) {
        return ((InternalDatasetDetails) dataset.getDatasetDetails()).getPartitioningKey();
    }

    public static String getNodegroupName(Dataset dataset) {
        return (((InternalDatasetDetails) dataset.getDatasetDetails())).getNodeGroupName();
    }

    public static int getPositionOfPartitioningKeyField(Dataset dataset, String fieldExpr) {
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (int i = 0; i < partitioningKeys.size(); i++) {
            if (partitioningKeys.get(i).equals(fieldExpr)) {
                return i;
            }
        }
        return -1;
    }
}
