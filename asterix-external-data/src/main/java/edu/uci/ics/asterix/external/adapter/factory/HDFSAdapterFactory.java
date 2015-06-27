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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.external.indexing.dataflow.HDFSObjectTupleParserFactory;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.InputSplitsFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

/**
 * A factory class for creating an instance of HDFSAdapter
 */
public class HDFSAdapterFactory extends StreamBasedAdapterFactory implements IAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String HDFS_ADAPTER_NAME = "hdfs";
    public static final String CLUSTER_LOCATIONS = "cluster-locations";
    public static transient String SCHEDULER = "hdfs-scheduler";

    public static final String KEY_HDFS_URL = "hdfs";
    public static final String KEY_PATH = "path";
    public static final String KEY_INPUT_FORMAT = "input-format";
    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";
    // New
    public static final String KEY_PARSER = "parser";
    public static final String PARSER_HIVE = "hive-parser";
    public static final String INPUT_FORMAT_RC = "rc-input-format";
    public static final String FORMAT_BINARY = "binary";

    private transient AlgebricksPartitionConstraint clusterLocations;
    private String[] readSchedule;
    private boolean executed[];
    private InputSplitsFactory inputSplitsFactory;
    private ConfFactory confFactory;
    private IAType atype;
    private boolean configured = false;
    public static Scheduler hdfsScheduler;
    private static boolean initialized = false;
    protected List<ExternalFile> files;

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

    protected static final Map<String, String> formatClassNames = initInputFormatMap();

    protected static Map<String, String> initInputFormatMap() {
        Map<String, String> formatClassNames = new HashMap<String, String>();
        formatClassNames.put(INPUT_FORMAT_TEXT, "org.apache.hadoop.mapred.TextInputFormat");
        formatClassNames.put(INPUT_FORMAT_SEQUENCE, "org.apache.hadoop.mapred.SequenceFileInputFormat");
        formatClassNames.put(INPUT_FORMAT_RC, "org.apache.hadoop.hive.ql.io.RCFileInputFormat");
        return formatClassNames;
    }

    public JobConf getJobConf() throws HyracksDataException {
        return confFactory.getConf();
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        JobConf conf = confFactory.getConf();
        InputSplit[] inputSplits = inputSplitsFactory.getSplits();
        String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
        HDFSAdapter hdfsAdapter = new HDFSAdapter(atype, readSchedule, executed, inputSplits, conf, nodeName,
                parserFactory, ctx, configuration, files);
        return hdfsAdapter;
    }

    @Override
    public String getName() {
        return HDFS_ADAPTER_NAME;
    }

    public static JobConf configureJobConf(Map<String, String> configuration) throws Exception {
        JobConf conf = new JobConf();
        String formatClassName = (String) formatClassNames.get(((String) configuration.get(KEY_INPUT_FORMAT)).trim());
        if (formatClassName == null) {
            formatClassName = ((String) configuration.get(KEY_INPUT_FORMAT)).trim();
        }
        conf.set("fs.default.name", ((String) configuration.get(KEY_HDFS_URL)).trim());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", ((String) configuration.get(KEY_PATH)).trim());
        conf.set("mapred.input.format.class", formatClassName);
        return conf;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (!configured) {
            throw new IllegalStateException("Adapter factory has not been configured yet");
        }
        return (AlgebricksPartitionConstraint) clusterLocations;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        if (!initialized) {
            hdfsScheduler = initializeHDFSScheduler();
            initialized = true;
        }
        this.configuration = configuration;
        JobConf conf = configureJobConf(configuration);
        confFactory = new ConfFactory(conf);

        clusterLocations = getClusterLocations();
        int numPartitions = ((AlgebricksAbsolutePartitionConstraint) clusterLocations).getLocations().length;

        // if files list was set, we restrict the splits to the list since this dataset is indexed
        InputSplit[] inputSplits;
        if (files == null) {
            inputSplits = conf.getInputFormat().getSplits(conf, numPartitions);
        } else {
            inputSplits = getSplits(conf);
        }
        inputSplitsFactory = new InputSplitsFactory(inputSplits);

        readSchedule = hdfsScheduler.getLocationConstraints(inputSplits);
        executed = new boolean[readSchedule.length];
        Arrays.fill(executed, false);
        configured = true;

        atype = (IAType) outputType;
        configureFormat(atype);
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    public static AlgebricksPartitionConstraint getClusterLocations() {
        ArrayList<String> locs = new ArrayList<String>();
        Map<String, String[]> stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
        for (String i : stores.keySet()) {
            String[] nodeStores = stores.get(i);
            int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(i);
            for (int j = 0; j < nodeStores.length; j++) {
                for (int k = 0; k < numIODevices; k++) {
                    locs.add(i);
                    locs.add(i);
                }
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return (ARecordType) atype;
    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.UNKNOWN;
    }

    /*
     * This method is overridden to do the following:
     * if data is text data (adm or delimited text), it will use a text tuple parser,
     * otherwise it will use hdfs record object parser
     */
    protected void configureFormat(IAType sourceDatatype) throws Exception {
         String specifiedFormat = (String) configuration.get(AsterixTupleParserFactory.KEY_FORMAT);
         if (specifiedFormat == null) {
                 throw new IllegalArgumentException(" Unspecified data format");
         } 
         
         if(AsterixTupleParserFactory.FORMAT_BINARY.equalsIgnoreCase(specifiedFormat)){
             parserFactory = new HDFSObjectTupleParserFactory((ARecordType) atype, this, configuration);
         } else {
             InputDataFormat inputFormat = InputDataFormat.UNKNOWN;
             if (AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT.equalsIgnoreCase(specifiedFormat)) {
                 inputFormat = InputDataFormat.DELIMITED;
             }   else if (AsterixTupleParserFactory.FORMAT_ADM.equalsIgnoreCase(specifiedFormat)) {
                 inputFormat = InputDataFormat.ADM;
             }    
             parserFactory = new AsterixTupleParserFactory(configuration, (ARecordType) sourceDatatype
                     , inputFormat);
         }  
        
     }

    /**
     * Instead of creating the split using the input format, we do it manually
     * This function returns fileSplits (1 per hdfs file block) irrespective of the number of partitions
     * and the produced splits only cover intersection between current files in hdfs and files stored internally
     * in AsterixDB
     * 1. NoOp means appended file
     * 2. AddOp means new file
     * 3. UpdateOp means the delta of a file
     *
     * @return
     * @throws IOException
     */
    protected InputSplit[] getSplits(JobConf conf) throws IOException {
        // Create file system object
        FileSystem fs = FileSystem.get(conf);
        ArrayList<FileSplit> fileSplits = new ArrayList<FileSplit>();
        ArrayList<ExternalFile> orderedExternalFiles = new ArrayList<ExternalFile>();
        // Create files splits
        for (ExternalFile file : files) {
            Path filePath = new Path(file.getFileName());
            FileStatus fileStatus;
            try {
                fileStatus = fs.getFileStatus(filePath);
            } catch (FileNotFoundException e) {
                // file was deleted at some point, skip to next file
                continue;
            }
            if (file.getPendingOp() == ExternalFilePendingOp.PENDING_ADD_OP
                    && fileStatus.getModificationTime() == file.getLastModefiedTime().getTime()) {
                // Get its information from HDFS name node
                BlockLocation[] fileBlocks = fs.getFileBlockLocations(fileStatus, 0, file.getSize());
                // Create a split per block
                for (BlockLocation block : fileBlocks) {
                    if (block.getOffset() < file.getSize()) {
                        fileSplits.add(new FileSplit(filePath, block.getOffset(), (block.getLength() + block
                                .getOffset()) < file.getSize() ? block.getLength() : (file.getSize() - block
                                .getOffset()), block.getHosts()));
                        orderedExternalFiles.add(file);
                    }
                }
            } else if (file.getPendingOp() == ExternalFilePendingOp.PENDING_NO_OP
                    && fileStatus.getModificationTime() == file.getLastModefiedTime().getTime()) {
                long oldSize = 0L;
                long newSize = file.getSize();
                for (int i = 0; i < files.size(); i++) {
                    if (files.get(i).getFileName() == file.getFileName() && files.get(i).getSize() != file.getSize()) {
                        newSize = files.get(i).getSize();
                        oldSize = file.getSize();
                        break;
                    }
                }

                // Get its information from HDFS name node
                BlockLocation[] fileBlocks = fs.getFileBlockLocations(fileStatus, 0, newSize);
                // Create a split per block
                for (BlockLocation block : fileBlocks) {
                    if (block.getOffset() + block.getLength() > oldSize) {
                        if (block.getOffset() < newSize) {
                            // Block interact with delta -> Create a split
                            long startCut = (block.getOffset() > oldSize) ? 0L : oldSize - block.getOffset();
                            long endCut = (block.getOffset() + block.getLength() < newSize) ? 0L : block.getOffset()
                                    + block.getLength() - newSize;
                            long splitLength = block.getLength() - startCut - endCut;
                            fileSplits.add(new FileSplit(filePath, block.getOffset() + startCut, splitLength, block
                                    .getHosts()));
                            orderedExternalFiles.add(file);
                        }
                    }
                }
            }
        }
        fs.close();
        files = orderedExternalFiles;
        return fileSplits.toArray(new FileSplit[fileSplits.size()]);
    }

    // Used to tell the factory to restrict the splits to the intersection between this list and the actual files on hdfs side
    public void setFiles(List<ExternalFile> files) {
        this.files = files;
    }

}
