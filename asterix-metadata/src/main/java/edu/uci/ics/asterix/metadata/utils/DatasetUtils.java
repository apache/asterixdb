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

package edu.uci.ics.asterix.metadata.utils;

import java.io.IOException;
import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class DatasetUtils {
	public static final String KEY_INPUT_FORMAT = "input-format";
	public static final String INPUT_FORMAT_RC = "rc-input-format";
    public static IBinaryComparatorFactory[] computeKeysBinaryComparatorFactories(Dataset dataset,
            ARecordType itemType, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<String> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); i++) {
            IAType keyType;
            try {
                keyType = itemType.getFieldType(partitioningKeys.get(i));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            bcfs[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        return bcfs;
    }

    public static int[] createBloomFilterKeyFields(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<String> partitioningKeys = getPartitioningKeys(dataset);
        int[] bloomFilterKeyFields = new int[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); ++i) {
            bloomFilterKeyFields[i] = i;
        }
        return bloomFilterKeyFields;
    }

    public static IBinaryHashFunctionFactory[] computeKeysBinaryHashFunFactories(Dataset dataset, ARecordType itemType,
            IBinaryHashFunctionFactoryProvider hashFunProvider) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<String> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); i++) {
            IAType keyType;
            try {
                keyType = itemType.getFieldType(partitioningKeys.get(i));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            bhffs[i] = hashFunProvider.getBinaryHashFunctionFactory(keyType);
        }
        return bhffs;
    }
    
    public static IBinaryHashFunctionFactory[] computeExternalDataKeysBinaryHashFunFactories(Dataset dataset,
			IBinaryHashFunctionFactoryProvider hashFunProvider) throws AlgebricksException {
    	if (dataset.getDatasetType() != DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
    	//get dataset details
    	ExternalDatasetDetails edd = (ExternalDatasetDetails) dataset.getDatasetDetails();	
    	if (edd.getProperties().get(KEY_INPUT_FORMAT).trim().equals(INPUT_FORMAT_RC))
    	{
    		//RID: <String(File name), Int64(Block byte location), Int32(row number)>
    		IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[3];
			bhffs[0] = hashFunProvider.getBinaryHashFunctionFactory(BuiltinType.ASTRING);
			bhffs[1] = hashFunProvider.getBinaryHashFunctionFactory(BuiltinType.AINT64);
			bhffs[2] = hashFunProvider.getBinaryHashFunctionFactory(BuiltinType.AINT32);
			return bhffs;
    	}
		else
		{
			//RID: <String(File name), Int64(Record byte location)>
			IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[2];
			bhffs[0] = hashFunProvider.getBinaryHashFunctionFactory(BuiltinType.ASTRING);
			bhffs[1] = hashFunProvider.getBinaryHashFunctionFactory(BuiltinType.AINT64);
			return bhffs;
		}
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
            IAType keyType;
            try {
                keyType = itemType.getFieldType(partitioningKeys.get(i));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
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

    public static int getExternalRIDSize(Dataset dataset) {
		ExternalDatasetDetails dsd = ((ExternalDatasetDetails) dataset.getDatasetDetails());
		if (dsd.getProperties().get(KEY_INPUT_FORMAT).equals(INPUT_FORMAT_RC))
		{
			return 3;
		}
		else{
			return 2;
		}
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
