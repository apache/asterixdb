/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixRuntimeUtil;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.hadoop.util.InputSplitsProxy;

/**
 * Provides functionality for fetching external data stored in an HDFS instance.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class HDFSAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(HDFSAdapter.class.getName());

    public static final String KEY_HDFS_URL = "hdfs";
    public static final String KEY_INPUT_FORMAT = "input-format";
    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";

    private Object[] inputSplits;
    private transient JobConf conf;
    private InputSplitsProxy inputSplitsProxy;
    private static final Map<String, String> formatClassNames = initInputFormatMap();

    private static Map<String, String> initInputFormatMap() {
        Map<String, String> formatClassNames = new HashMap<String, String>();
        formatClassNames.put(INPUT_FORMAT_TEXT, "org.apache.hadoop.mapred.TextInputFormat");
        formatClassNames.put(INPUT_FORMAT_SEQUENCE, "org.apache.hadoop.mapred.SequenceFileInputFormat");
        return formatClassNames;
    }

    public HDFSAdapter(IAType atype) {
        super(atype);
    }

    @Override
    public void configure(Map<String, String> arguments) throws Exception {
        configuration = arguments;
        configureFormat();
        configureJobConf();
        configureSplits();
    }

    private void configureSplits() throws IOException {
        if (inputSplitsProxy == null) {
            inputSplits = conf.getInputFormat().getSplits(conf, 0);
        }
        inputSplitsProxy = new InputSplitsProxy(conf, inputSplits);
    }

    private void configurePartitionConstraint() throws Exception {
        List<String> locations = new ArrayList<String>();
        Random random = new Random();
        boolean couldConfigureLocationConstraints = false;
        try {
            Map<String, Set<String>> nodeControllers = AsterixRuntimeUtil.getNodeControllerMap();
            for (Object inputSplit : inputSplits) {
                String[] dataNodeLocations = ((InputSplit) inputSplit).getLocations();
                if (dataNodeLocations == null || dataNodeLocations.length == 0) {
                    throw new IllegalArgumentException("No datanode locations found: check hdfs path");
                }

                // loop over all replicas until a split location coincides
                // with an asterix datanode location
                for (String datanodeLocation : dataNodeLocations) {
                    Set<String> nodeControllersAtLocation = null;
                    try {
                        nodeControllersAtLocation = nodeControllers.get(AsterixRuntimeUtil
                                .getIPAddress(datanodeLocation));
                    } catch (UnknownHostException uhe) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.log(Level.WARNING, "Unknown host :" + datanodeLocation);
                        }
                        continue;
                    }
                    if (nodeControllersAtLocation == null || nodeControllersAtLocation.size() == 0) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.log(Level.WARNING, "No node controller found at " + datanodeLocation
                                    + " will look at replica location");
                        }
                        couldConfigureLocationConstraints = false;
                    } else {
                        int locationIndex = random.nextInt(nodeControllersAtLocation.size());
                        String chosenLocation = (String) nodeControllersAtLocation.toArray()[locationIndex];
                        locations.add(chosenLocation);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.log(Level.INFO, "split : " + inputSplit + " to be processed by :" + chosenLocation);
                        }
                        couldConfigureLocationConstraints = true;
                        break;
                    }
                }

                /* none of the replica locations coincides with an Asterix
                   node controller location.
                */
                if (!couldConfigureLocationConstraints) {
                    List<String> allNodeControllers = AsterixRuntimeUtil.getAllNodeControllers();
                    int locationIndex = random.nextInt(allNodeControllers.size());
                    String chosenLocation = allNodeControllers.get(locationIndex);
                    locations.add(chosenLocation);
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.log(Level.SEVERE, "No local node controller found to process split : " + inputSplit
                                + " will be processed by a remote node controller:" + chosenLocation);
                    }
                }
            }
            partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[] {}));
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, "Encountered exception :" + e + " using count constraints");
            }
            partitionConstraint = new AlgebricksCountPartitionConstraint(inputSplits.length);
        }
    }

    private JobConf configureJobConf() throws Exception {
        conf = new JobConf();
        conf.set("fs.default.name", configuration.get(KEY_HDFS_URL).trim());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", configuration.get(KEY_PATH).trim());
        conf.set("mapred.input.format.class", formatClassNames.get(configuration.get(KEY_INPUT_FORMAT).trim()));
        return conf;
    }

    public AdapterType getAdapterType() {
        return AdapterType.READ_WRITE;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
        inputSplits = inputSplitsProxy.toInputSplits(conf);
    }

    private Reporter getReporter() {
        Reporter reporter = new Reporter() {

            @Override
            public Counter getCounter(Enum<?> arg0) {
                return null;
            }

            @Override
            public Counter getCounter(String arg0, String arg1) {
                return null;
            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> arg0, long arg1) {
            }

            @Override
            public void incrCounter(String arg0, String arg1, long arg2) {
            }

            @Override
            public void setStatus(String arg0) {
            }

            @Override
            public void progress() {
            }
        };

        return reporter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputStream getInputStream(int partition) throws IOException {
        try {
            InputStream inputStream;
            if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
                SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
                RecordReader reader = format.getRecordReader(
                        (org.apache.hadoop.mapred.FileSplit) inputSplits[partition], conf, getReporter());
                inputStream = new HDFSStream(reader, ctx);
            } else {
                try {
                    TextInputFormat format = (TextInputFormat) conf.getInputFormat();
                    RecordReader reader = format.getRecordReader(
                            (org.apache.hadoop.mapred.FileSplit) inputSplits[partition], conf, getReporter());
                    inputStream = new HDFSStream(reader, ctx);
                } catch (FileNotFoundException e) {
                    throw new HyracksDataException(e);
                }
            }
            return inputStream;
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (partitionConstraint == null) {
            configurePartitionConstraint();
        }
        return partitionConstraint;
    }

}

class HDFSStream extends InputStream {

    private RecordReader<Object, Text> reader;
    private final Object key;
    private final Text value;
    private boolean hasMore = false;
    private static final int EOL = "\n".getBytes()[0];
    private Text pendingValue = null;

    public HDFSStream(RecordReader<Object, Text> reader, IHyracksTaskContext ctx) throws Exception {
        this.reader = reader;
        key = reader.createKey();
        try {
            value = (Text) reader.createValue();
        } catch (ClassCastException cce) {
            throw new Exception("value is not of type org.apache.hadoop.io.Text"
                    + " type not supported in sequence file format", cce);
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        int numBytes = 0;
        if (pendingValue != null) {
            System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.getLength());
            buffer[offset + numBytes + pendingValue.getLength()] = (byte) EOL;
            numBytes += pendingValue.getLength() + 1;
            pendingValue = null;
        }

        while (numBytes < len) {
            hasMore = reader.next(key, value);
            if (!hasMore) {
                return (numBytes == 0) ? -1 : numBytes;
            }
            int sizeOfNextTuple = value.getLength() + 1;
            if (numBytes + sizeOfNextTuple > len) {
                // cannot add tuple to current buffer
                // but the reader has moved pass the fetched tuple
                // we need to store this for a subsequent read call.
                // and return this then.
                pendingValue = value;
                break;
            } else {
                System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
                buffer[offset + numBytes + value.getLength()] = (byte) EOL;
                numBytes += sizeOfNextTuple;
            }
        }
        return numBytes;
    }

    @Override
    public int read() throws IOException {
        throw new NotImplementedException("Use read(byte[], int, int");
    }

}