/*
 * Copyright 2009-2011 by The Regents of the University of California
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.util.AsterixRuntimeUtil;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.hadoop.util.InputSplitsProxy;

public class HDFSAdapter extends FileSystemBasedAdapter {

    private static final Logger LOGGER = Logger.getLogger(HDFSAdapter.class.getName());

    private Object[] inputSplits;
    private transient JobConf conf;
    private InputSplitsProxy inputSplitsProxy;
    private static final Map<String, String> formatClassNames = new HashMap<String, String>();

    public static final String KEY_HDFS_URL = "hdfs";
    public static final String KEY_HDFS_PATH = "path";
    public static final String KEY_INPUT_FORMAT = "input-format";

    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";

    static {
        formatClassNames.put(INPUT_FORMAT_TEXT, "org.apache.hadoop.mapred.TextInputFormat");
        formatClassNames.put(INPUT_FORMAT_SEQUENCE, "org.apache.hadoop.mapred.SequenceFileInputFormat");
    }

    public HDFSAdapter(IAType atype) {
        super(atype);
    }

    @Override
    public void configure(Map<String, String> arguments) throws Exception {
        configuration = arguments;
        configureFormat();
        configureJobConf();
        configurePartitionConstraint();
    }

    private void configurePartitionConstraint() throws Exception {
        AlgebricksAbsolutePartitionConstraint absPartitionConstraint;
        List<String> locations = new ArrayList<String>();
        Random random = new Random();
        boolean couldConfigureLocationConstraints = true;
        if (inputSplitsProxy == null) {
            InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, 0);
            try {
                for (InputSplit inputSplit : inputSplits) {
                    String[] dataNodeLocations = inputSplit.getLocations();
                    // loop over all replicas until a split location coincides
                    // with an asterix datanode location
                    for (String datanodeLocation : dataNodeLocations) {
                        Set<String> nodeControllersAtLocation = AsterixRuntimeUtil
                                .getNodeControllersOnHostName(datanodeLocation);
                        if (nodeControllersAtLocation == null || nodeControllersAtLocation.size() == 0) {
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.log(Level.INFO, "No node controller found at " + datanodeLocation
                                        + " will look at replica location");
                            }
                            couldConfigureLocationConstraints = false;
                        } else {
                            int locationIndex = random.nextInt(nodeControllersAtLocation.size());
                            String chosenLocation = (String) nodeControllersAtLocation.toArray()[locationIndex];
                            locations.add(chosenLocation);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.log(Level.INFO, "split : " + inputSplit + " to be processed by :"
                                        + chosenLocation);
                            }
                            couldConfigureLocationConstraints = true;
                            break;
                        }
                    }

                    // none of the replica locations coincides with an Asterix
                    // node controller location.
                    if (!couldConfigureLocationConstraints) {
                        List<String> allNodeControllers = AsterixRuntimeUtil.getAllNodeControllers();
                        int locationIndex = random.nextInt(allNodeControllers.size());
                        String chosenLocation = allNodeControllers.get(locationIndex);
                        locations.add(chosenLocation);

                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.log(Level.INFO, "No local node controller found to process split : " + inputSplit
                                    + " will be processed by a remote node controller:" + chosenLocation);
                        }
                        break;
                    }
                }
                if (couldConfigureLocationConstraints) {
                    partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[] {}));
                } else {
                    partitionConstraint = new AlgebricksCountPartitionConstraint(inputSplits.length);
                }
            } catch (UnknownHostException e) {
                partitionConstraint = new AlgebricksCountPartitionConstraint(inputSplits.length);
            }
            inputSplitsProxy = new InputSplitsProxy(conf, inputSplits);
        }
    }

    private JobConf configureJobConf() throws Exception {
        conf = new JobConf();
        conf.set("fs.default.name", configuration.get(KEY_HDFS_URL));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", configuration.get(KEY_HDFS_PATH));
        conf.set("mapred.input.format.class", formatClassNames.get(configuration.get(KEY_INPUT_FORMAT)));
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

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        Path path = new Path(inputSplits[partition].toString());
        try {
            FileSystem fs = FileSystem.get(conf);
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

}

class HDFSStream extends InputStream {

    private ByteBuffer buffer;
    private int capacity;
    private RecordReader reader;
    private boolean readNext = true;
    private final Object key;
    private final Text value;

    public HDFSStream(RecordReader reader, IHyracksTaskContext ctx) throws Exception {
        capacity = ctx.getFrameSize();
        buffer = ByteBuffer.allocate(capacity);
        this.reader = reader;
        key = reader.createKey();
        try {
            value = (Text) reader.createValue();
        } catch (ClassCastException cce) {
            throw new Exception("context is not of type org.apache.hadoop.io.Text"
                    + " type not supported in sequence file format", cce);
        }
        initialize();
    }

    private void initialize() throws Exception {
        boolean hasMore = reader.next(key, value);
        if (!hasMore) {
            buffer.limit(0);
        } else {
            buffer.position(0);
            buffer.limit(capacity);
            buffer.put(value.getBytes());
            buffer.put("\n".getBytes());
            buffer.flip();
        }
    }

    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            boolean hasMore = reader.next(key, value);
            if (!hasMore) {
                return -1;
            }
            buffer.position(0);
            buffer.limit(capacity);
            buffer.put(value.getBytes());
            buffer.put("\n".getBytes());
            buffer.flip();
            return buffer.get();
        } else {
            return buffer.get();
        }

    }

}