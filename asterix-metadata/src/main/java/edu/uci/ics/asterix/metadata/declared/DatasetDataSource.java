package edu.uci.ics.asterix.metadata.declared;

import java.io.IOException;
import java.util.List;

import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class DatasetDataSource extends AqlDataSource {

    private Dataset dataset;

    public DatasetDataSource(AqlSourceId id, String datasourceDataverse, String datasourceName, IAType itemType,
            AqlDataSourceType datasourceType) throws AlgebricksException {
        super(id, datasourceDataverse, datasourceName, itemType, datasourceType);
        MetadataTransactionContext ctx = null;
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            dataset = MetadataManager.INSTANCE.getDataset(ctx, datasourceDataverse, datasourceName);
            if (dataset == null) {
                throw new AlgebricksException("Unknown dataset " + datasourceName + " in dataverse "
                        + datasourceDataverse);
            }
            MetadataManager.INSTANCE.commitTransaction(ctx);
            switch (dataset.getDatasetType()) {
                case INTERNAL:
                    initInternalDataset(itemType);
                    break;
                case EXTERNAL:
                    initExternalDataset(itemType);
                    break;

            }
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    throw new IllegalStateException("Unable to abort " + e2.getMessage());
                }
            }

        }

    }

    public Dataset getDataset() {
        return dataset;
    }

    private void initInternalDataset(IAType itemType) throws IOException, AlgebricksException {
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        ARecordType recordType = (ARecordType) itemType;
        int n = partitioningKeys.size();
        schemaTypes = new IAType[n + 1];
        for (int i = 0; i < n; i++) {
            schemaTypes[i] = recordType.getSubFieldType(partitioningKeys.get(i));
        }
        schemaTypes[n] = itemType;
        domain = new DefaultNodeGroupDomain(DatasetUtils.getNodegroupName(dataset));
    }

    private void initExternalDataset(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
        INodeDomain domainForExternalData = new INodeDomain() {
            @Override
            public Integer cardinality() {
                return null;
            }

            @Override
            public boolean sameAs(INodeDomain domain) {
                return domain == this;
            }
        };
        domain = domainForExternalData;
    }

    @Override
    public IAType[] getSchemaTypes() {
        return schemaTypes;
    }

    @Override
    public INodeDomain getDomain() {
        return domain;
    }

    @Override
    public void computeLocalStructuralProperties(List<ILocalStructuralProperty> localProps,
            List<LogicalVariable> variables) {
        // do nothing
    }

}
