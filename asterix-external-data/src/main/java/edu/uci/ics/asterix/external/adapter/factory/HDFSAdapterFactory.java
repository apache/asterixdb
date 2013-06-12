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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.InputSplitsFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

/**
 * A factory class for creating an instance of HDFSAdapter
 */
@SuppressWarnings("deprecation")
public class HDFSAdapterFactory extends FileSystemAdapterFactory {
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
    private IAType atype;
    private boolean configured = false;
    public static Scheduler hdfsScheduler = initializeHDFSScheduler();

    private static Scheduler initializeHDFSScheduler() {
        ICCContext ccContext = AsterixAppContextInfo.getInstance().getCCApplicationContext().getCCContext();
        Scheduler scheduler = null;
        try {
            scheduler = new Scheduler(ccContext.getClusterControllerInfo().getClientNetAddress(), ccContext
                    .getClusterControllerInfo().getClientNetPort());
        } catch (HyracksException e) {
            throw new IllegalStateException("Cannot obtain hdfs scheduler");
        }
        return scheduler;
    }

    private static final Map<String, String> formatClassNames = initInputFormatMap();

    private static Map<String, String> initInputFormatMap() {
        Map<String, String> formatClassNames = new HashMap<String, String>();
        formatClassNames.put(INPUT_FORMAT_TEXT, "org.apache.hadoop.mapred.TextInputFormat");
        formatClassNames.put(INPUT_FORMAT_SEQUENCE, "org.apache.hadoop.mapred.SequenceFileInputFormat");
        return formatClassNames;
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        JobConf conf = confFactory.getConf();
        InputSplit[] inputSplits = inputSplitsFactory.getSplits();
        String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
        HDFSAdapter hdfsAdapter = new HDFSAdapter(atype, readSchedule, executed, inputSplits, conf, nodeName,
                parserFactory, ctx);
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

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.GENERIC;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (!configured) {
            throw new IllegalStateException("Adapter factory has not been configured yet");
        }
        return (AlgebricksPartitionConstraint) clusterLocations;
    }

    @Override
    public void configure(Map<String, Object> configuration) throws Exception {
        this.configuration = configuration;
        JobConf conf = configureJobConf(configuration);
        confFactory = new ConfFactory(conf);

        clusterLocations = getClusterLocations();
        int numPartitions = ((AlgebricksAbsolutePartitionConstraint) clusterLocations).getLocations().length;

        InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, numPartitions);
        inputSplitsFactory = new InputSplitsFactory(inputSplits);

        readSchedule = hdfsScheduler.getLocationConstraints(inputSplits);
        executed = new boolean[readSchedule.length];
        Arrays.fill(executed, false);
        configured = true;

        atype = (IAType) configuration.get(KEY_SOURCE_DATATYPE);
        configureFormat(atype);
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    private static AlgebricksPartitionConstraint getClusterLocations() {
        ArrayList<String> locs = new ArrayList<String>();
        Map<String, String[]> stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
        for (String i : stores.keySet()) {
            String[] nodeStores = stores.get(i);
            int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(i);
            for (int j = 0; j < nodeStores.length; j++) {
                for (int k = 0; k < numIODevices; k++) {
                    locs.add(i);
                }
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }

}
