package edu.uci.ics.asterix.metadata.declared;

import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ExternalFeedDataSource extends AqlDataSource {

    public ExternalFeedDataSource(AqlSourceId id, Dataset dataset, IAType itemType) throws AlgebricksException {
        super(id, dataset, itemType);
    }

    public ExternalFeedDataSource(AqlSourceId id, Dataset dataset, IAType itemType, AqlDataSourceType dataSourceType)
            throws AlgebricksException {
        super(id, dataset, itemType, dataSourceType);
    }
}
