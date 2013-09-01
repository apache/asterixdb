package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.Map;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Provides the functionality of fetching data in form of ADM records from a Hive dataset.
 */
@SuppressWarnings("deprecation")
public class HiveIndexingAdapter extends AbstractDatasourceAdapter{

    private static final long serialVersionUID = 1L;

    public static final String HIVE_DATABASE = "database";
    public static final String HIVE_TABLE = "table";
    public static final String HIVE_HOME = "hive-home";
    public static final String HIVE_METASTORE_URI = "metastore-uri";
    public static final String HIVE_WAREHOUSE_DIR = "warehouse-dir";
    public static final String HIVE_METASTORE_RAWSTORE_IMPL = "rawstore-impl";

    private HDFSIndexingAdapter hdfsIndexingAdapter;

    public HiveIndexingAdapter(IAType atype, String[] readSchedule, boolean[] executed, InputSplit[] inputSplits, JobConf conf,
            AlgebricksPartitionConstraint clusterLocations, Map<String,Integer> files) {
        this.hdfsIndexingAdapter = new HDFSIndexingAdapter(atype, readSchedule, executed, inputSplits, conf, clusterLocations, files);
        this.atype = atype;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        this.configuration = arguments;
        this.hdfsIndexingAdapter.configure(arguments);
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
    	hdfsIndexingAdapter.initialize(ctx);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
    	hdfsIndexingAdapter.start(partition, writer);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return hdfsIndexingAdapter.getPartitionConstraint();
    }

}