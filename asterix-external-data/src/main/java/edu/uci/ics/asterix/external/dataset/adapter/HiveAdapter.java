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
public class HiveAdapter extends AbstractDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    public static final String HIVE_DATABASE = "database";
    public static final String HIVE_TABLE = "table";
    public static final String HIVE_HOME = "hive-home";
    public static final String HIVE_METASTORE_URI = "metastore-uri";
    public static final String HIVE_WAREHOUSE_DIR = "warehouse-dir";
    public static final String HIVE_METASTORE_RAWSTORE_IMPL = "rawstore-impl";

    private HDFSAdapter hdfsAdapter;

    public HiveAdapter(IAType atype, String[] readSchedule, boolean[] executed, InputSplit[] inputSplits, JobConf conf,
            AlgebricksPartitionConstraint clusterLocations) {
        this.hdfsAdapter = new HDFSAdapter(atype, readSchedule, executed, inputSplits, conf, clusterLocations);
        this.atype = atype;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        this.configuration = arguments;
        this.hdfsAdapter.configure(arguments);
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        hdfsAdapter.initialize(ctx);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        hdfsAdapter.start(partition, writer);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return hdfsAdapter.getPartitionConstraint();
    }

}
