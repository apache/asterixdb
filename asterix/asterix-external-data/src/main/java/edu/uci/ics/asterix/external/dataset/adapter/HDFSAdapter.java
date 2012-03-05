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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.external.data.parser.DelimitedDataStreamParser;
import edu.uci.ics.asterix.external.data.parser.IDataParser;
import edu.uci.ics.asterix.external.data.parser.IDataStreamParser;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class HDFSAdapter extends AbstractDatasourceAdapter implements IDatasourceReadAdapter {

    private String hdfsUrl;
    private List<String> hdfsPaths;
    private String inputFormatClassName;
    private InputSplit[] inputSplits;
    private JobConf conf;
    private IHyracksTaskContext ctx;
    private Reporter reporter;
    private boolean isDelimited;
    private Character delimiter;
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

    public String getHdfsUrl() {
        return hdfsUrl;
    }

    public void setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    public List<String> getHdfsPaths() {
        return hdfsPaths;
    }

    public void setHdfsPaths(List<String> hdfsPaths) {
        this.hdfsPaths = hdfsPaths;
    }

    @Override
    public void configure(Map<String, String> arguments, IAType atype) throws Exception {
        configuration = arguments;
        configureFormat();
        configureJobConf();
        configurePartitionConstraint();
        this.atype = atype;
    }

    private void configureFormat() throws Exception {
        String format = configuration.get(KEY_INPUT_FORMAT);
        inputFormatClassName = formatClassNames.get(format);
        if (inputFormatClassName == null) {
            throw new Exception("format " + format + " not supported");
        }

        String parserClass = configuration.get(KEY_PARSER);
        if (parserClass == null) {
            if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parserClass = formatToParserMap.get(FORMAT_DELIMITED_TEXT);
            } else if (FORMAT_ADM.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parserClass = formatToParserMap.get(FORMAT_ADM);
            }
        }

        dataParser = (IDataParser) Class.forName(parserClass).newInstance();
        dataParser.configure(configuration);
    }

    private void configurePartitionConstraint() throws Exception {
        InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, 0);
        partitionConstraint = new AlgebricksCountPartitionConstraint(inputSplits.length);
        hdfsPaths = new ArrayList<String>();
        for (String hdfsPath : configuration.get(KEY_HDFS_PATH).split(",")) {
            hdfsPaths.add(hdfsPath);
        }
    }

    private ITupleParserFactory createTupleParserFactory(ARecordType recType) {
        if (isDelimited) {
            int n = recType.getFieldTypes().length;
            IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
            for (int i = 0; i < n; i++) {
                ATypeTag tag = recType.getFieldTypes()[i].getTypeTag();
                IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
                if (vpf == null) {
                    throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
                }
                fieldParserFactories[i] = vpf;
            }
            return new NtDelimitedDataTupleParserFactory(recType, fieldParserFactories, delimiter);
        } else {
            return new AdmSchemafullRecordParserFactory(recType);
        }
    }

    private JobConf configureJobConf() throws Exception {
        hdfsUrl = configuration.get(KEY_HDFS_URL);
        conf = new JobConf();
        conf.set("fs.default.name", hdfsUrl);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(HDFSAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", configuration.get(KEY_HDFS_PATH));
        conf.set("mapred.input.format.class", formatClassNames.get(configuration.get(KEY_INPUT_FORMAT)));
        return conf;
    }

    public AdapterDataFlowType getAdapterDataFlowType() {
        return AdapterDataFlowType.PULL;
    }

    public AdapterType getAdapterType() {
        return AdapterType.READ_WRITE;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
        inputSplits = conf.getInputFormat().getSplits(conf, 0);
        dataParser.initialize((ARecordType) atype, ctx);
        reporter = new Reporter() {

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
    }

    @Override
    public IDataParser getDataParser(int partition) throws Exception {
        Path path = new Path(inputSplits[partition].toString());
        FileSystem fs = FileSystem.get(conf);
        InputStream inputStream;
        if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
            SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
            RecordReader reader = format.getRecordReader(inputSplits[partition], conf, reporter);
            inputStream = new SequenceToTextStream(reader, ctx);
        } else {
            try {
                inputStream = fs.open(((org.apache.hadoop.mapred.FileSplit) inputSplits[partition]).getPath());
            } catch (FileNotFoundException e) {
                throw new HyracksDataException(e);
            }
        }

        if (dataParser instanceof IDataStreamParser) {
            ((IDataStreamParser) dataParser).setInputStream(inputStream);
        } else {
            throw new IllegalArgumentException(" parser not compatible");
        }

        return dataParser;
    }

}

class SequenceToTextStream extends InputStream {

    private ByteBuffer buffer;
    private int capacity;
    private RecordReader reader;
    private boolean readNext = true;
    private final Object key;
    private final Text value;

    public SequenceToTextStream(RecordReader reader, IHyracksTaskContext ctx) throws Exception {
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