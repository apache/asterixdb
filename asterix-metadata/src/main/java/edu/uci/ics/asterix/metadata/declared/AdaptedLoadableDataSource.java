package edu.uci.ics.asterix.metadata.declared;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.INodeDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;

public class AdaptedLoadableDataSource extends AqlDataSource {

    private final INodeDomain domain;

    private final IAType[] schemaTypes;

    private final Dataset targetDataset;

    private final List<String> partitioningKeys;

    private final String adapter;

    private final Map<String, String> adapterProperties;

    private final boolean alreadySorted;

    public AdaptedLoadableDataSource(Dataset targetDataset, IAType itemType, String adapter,
            Map<String, String> properties, boolean alreadySorted) throws AlgebricksException, IOException {
        super(new AqlSourceId("loadable_dv", "loadable_ds"), "loadable_dv", "loadable_source",
                AqlDataSourceType.ADAPTED_LOADABLE);
        this.targetDataset = targetDataset;
        this.adapter = adapter;
        this.adapterProperties = properties;
        this.alreadySorted = alreadySorted;
        partitioningKeys = DatasetUtils.getPartitioningKeys(targetDataset);
        domain = new DefaultNodeGroupDomain(DatasetUtils.getNodegroupName(targetDataset));
        ARecordType recType = (ARecordType) itemType;
        schemaTypes = new IAType[partitioningKeys.size() + 1];
        for (int i = 0; i < partitioningKeys.size(); ++i) {
            schemaTypes[i] = recType.getFieldType(partitioningKeys.get(i));
        }
        schemaTypes[schemaTypes.length - 1] = itemType;
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
        if (alreadySorted) {
            List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
            for (int i = 0; i < partitioningKeys.size(); ++i) {
                orderColumns.add(new OrderColumn(variables.get(i), OrderKind.ASC));
            }
            localProps.add(new LocalOrderProperty(orderColumns));
        }
    }

    public List<String> getPartitioningKeys() {
        return partitioningKeys;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getAdapterProperties() {
        return adapterProperties;
    }

    public IAType getTargetDatasetType() {
        return schemaTypes[schemaTypes.length - 1];
    }

    public Dataset getTargetDataset() {
        return targetDataset;
    }

}
