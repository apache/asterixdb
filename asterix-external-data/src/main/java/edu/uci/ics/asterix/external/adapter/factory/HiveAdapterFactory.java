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
import edu.uci.ics.asterix.external.dataset.adapter.HiveAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.InputSplitsFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

/**
 * A factory class for creating an instance of HiveAdapter
 */
@SuppressWarnings("deprecation")
public class HiveAdapterFactory implements IGenericDatasetAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String HDFS_ADAPTER_NAME = "hdfs";
    public static final String CLUSTER_LOCATIONS = "cluster-locations";
    public static transient String SCHEDULER = "hdfs-scheduler";

    public static final String KEY_HDFS_URL = "hdfs";
    public static final String KEY_PATH = "path";
    public static final String KEY_INPUT_FORMAT = "input-format";
    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";

    public static final String KEY_FORMAT = "format";
    public static final String KEY_PARSER_FACTORY = "parser";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_ADM = "adm";

    public static final String HIVE_DATABASE = "database";
    public static final String HIVE_TABLE = "table";
    public static final String HIVE_HOME = "hive-home";
    public static final String HIVE_METASTORE_URI = "metastore-uri";
    public static final String HIVE_WAREHOUSE_DIR = "warehouse-dir";
    public static final String HIVE_METASTORE_RAWSTORE_IMPL = "rawstore-impl";

    private String[] readSchedule;
    private boolean executed[];
    private InputSplitsFactory inputSplitsFactory;
    private ConfFactory confFactory;
    private transient AlgebricksPartitionConstraint clusterLocations;
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
        HiveAdapter hiveAdapter = new HiveAdapter(atype, readSchedule, executed, inputSplits, conf, clusterLocations);
        hiveAdapter.configure(configuration);
        return hiveAdapter;
    }

    @Override
    public String getName() {
        return "hive";
    }

    private JobConf configureJobConf(Map<String, Object> configuration) throws Exception {
        JobConf conf = new JobConf();

        /** configure hive */
        String database = (String) configuration.get(HIVE_DATABASE);
        String tablePath = null;
        if (database == null) {
            tablePath = configuration.get(HIVE_WAREHOUSE_DIR) + "/" + configuration.get(HIVE_TABLE);
        } else {
            tablePath = configuration.get(HIVE_WAREHOUSE_DIR) + "/" + tablePath + ".db" + "/"
                    + configuration.get(HIVE_TABLE);
        }
        configuration.put(HDFSAdapter.KEY_PATH, tablePath);
        if (!configuration.get(KEY_FORMAT).equals(FORMAT_DELIMITED_TEXT)) {
            throw new IllegalArgumentException("format" + configuration.get(KEY_FORMAT) + " is not supported");
        }

        if (!(configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT) || configuration
                .get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_SEQUENCE))) {
            throw new IllegalArgumentException("file input format"
                    + configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT) + " is not supported");
        }

        /** configure hdfs */
        conf.set("fs.default.name", ((String) configuration.get(KEY_HDFS_URL)).trim());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", ((String) configuration.get(KEY_PATH)).trim());
        conf.set("mapred.input.format.class",
                (String) formatClassNames.get(((String) configuration.get(KEY_INPUT_FORMAT)).trim()));
        return conf;
    }
}
