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

package edu.uci.ics.hyracks.hdfs.dataflow;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class HDFSReadOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private ConfFactory confFactory;
    private InputSplitsFactory splitsFactory;
    private String[] scheduledLocations;
    private IKeyValueParserFactory tupleParserFactory;

    public HDFSReadOperatorDescriptor(JobSpecification spec, RecordDescriptor rd, JobConf conf, InputSplit[] splits,
            String[] scheduledLocations, IKeyValueParserFactory tupleParserFactory) throws HyracksException {
        super(spec, 0, 1);
        try {
            this.splitsFactory = new InputSplitsFactory(splits);
            this.confFactory = new ConfFactory(conf);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
        this.scheduledLocations = scheduledLocations;
        this.tupleParserFactory = tupleParserFactory;
        this.recordDescriptors[0] = rd;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final InputSplit[] inputSplits = splitsFactory.getSplits();
        final JobConf conf = confFactory.getConf();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                try {
                    IKeyValueParser parser = tupleParserFactory.createKeyValueParser(ctx);
                    writer.open();
                    InputFormat inputFormat = conf.getInputFormat();
                    for (int i = 0; i < inputSplits.length; i++) {
                        /**
                         * read all the partitions scheduled to the current node
                         */
                        if (scheduledLocations[i].equals(nodeName)) {
                            RecordReader reader = inputFormat.getRecordReader(inputSplits[i], conf, Reporter.NULL);
                            Object key = reader.createKey();
                            Object value = reader.createValue();
                            while (reader.next(key, value) == true) {
                                parser.parse(key, value, writer);
                            }
                        }
                    }
                    parser.flush(writer);
                    writer.close();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
