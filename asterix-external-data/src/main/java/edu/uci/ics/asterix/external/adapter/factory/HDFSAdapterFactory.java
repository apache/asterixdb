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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.InputSplitsFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

/**
 * A factory class for creating an instance of HDFSAdapter
 */
@SuppressWarnings("deprecation")
public class HDFSAdapterFactory implements IGenericDatasetAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String HDFS_ADAPTER_NAME = "hdfs";
    public static final String CLUSTER_LOCATIONS = "cluster-locations";
    public static transient String SCHEDULER = "hdfs-scheduler";

    public static final String KEY_HDFS_URL = "hdfs";
    public static final String KEY_PATH = "path";
    public static final String KEY_INPUT_FORMAT = "input-format";
    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";

    private transient AlgebricksPartitionConstraint clusterLocations;
    private String[] readSchedule;
    private boolean executed[];
    private InputSplitsFactory inputSplitsFactory;
    private ConfFactory confFactory;
    private boolean setup = false;

    private static final Map<String, String> formatClassNames = initInputFormatMap();

    private static Map<String, String> initInputFormatMap() {
        Map<String, String> formatClassNames = new HashMap<String, String>();
        formatClassNames.put(INPUT_FORMAT_TEXT, "org.apache.hadoop.mapred.TextInputFormat");
        formatClassNames.put(INPUT_FORMAT_SEQUENCE, "org.apache.hadoop.mapred.SequenceFileInputFormat");
        return formatClassNames;
    }

    @Override
    public IDatasourceAdapter createAdapter(Map<String, Object> configuration, IAType atype) throws Exception {
        if (!setup) {
            /** set up the factory --serializable stuff --- this if-block should be called only once for each factory instance */
            configureJobConf(configuration);
            JobConf conf = configureJobConf(configuration);
            confFactory = new ConfFactory(conf);

            clusterLocations = (AlgebricksPartitionConstraint) configuration.get(CLUSTER_LOCATIONS);
            int numPartitions = ((AlgebricksAbsolutePartitionConstraint) clusterLocations).getLocations().length;

            InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, numPartitions);
            inputSplitsFactory = new InputSplitsFactory(inputSplits);

            Scheduler scheduler = (Scheduler) configuration.get(SCHEDULER);
            readSchedule = scheduler.getLocationConstraints(inputSplits);
            executed = new boolean[readSchedule.length];
            Arrays.fill(executed, false);

            setup = true;
        }
        JobConf conf = confFactory.getConf();
        InputSplit[] inputSplits = inputSplitsFactory.getSplits();
        HDFSAdapter hdfsAdapter = new HDFSAdapter(atype, readSchedule, executed, inputSplits, conf, clusterLocations);
        hdfsAdapter.configure(configuration);
        return hdfsAdapter;
    }

    @Override
    public String getName() {
        return HDFS_ADAPTER_NAME;
    }

    private JobConf configureJobConf(Map<String, Object> configuration) throws Exception {
        JobConf conf = new JobConf();
        conf.set("fs.default.name", ((String) configuration.get(KEY_HDFS_URL)).trim());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", ((String) configuration.get(KEY_PATH)).trim());
        conf.set("mapred.input.format.class",
                (String) formatClassNames.get(((String) configuration.get(KEY_INPUT_FORMAT)).trim()));
        return conf;
    }

}
