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

package edu.uci.ics.hyracks.hdfs.dataflow;

import java.util.Arrays;

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

/**
 * The HDFS file read operator using the Hadoop old API.
 * To use this operator, a user need to provide an IKeyValueParserFactory implementation which convert
 * key-value pairs into tuples.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class HDFSReadOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final ConfFactory confFactory;
    private final InputSplitsFactory splitsFactory;
    private final String[] scheduledLocations;
    private final IKeyValueParserFactory tupleParserFactory;
    private final boolean[] executed;

    /**
     * The constructor of HDFSReadOperatorDescriptor.
     * 
     * @param spec
     *            the JobSpecification object
     * @param rd
     *            the output record descriptor
     * @param conf
     *            the Hadoop JobConf object, which contains the input format and the input paths
     * @param splits
     *            the array of FileSplits (HDFS chunks).
     * @param scheduledLocations
     *            the node controller names to scan the FileSplits, which is an one-to-one mapping. The String array
     *            is obtained from the edu.cui.ics.hyracks.hdfs.scheduler.Scheduler.getLocationConstraints(InputSplits[]).
     * @param tupleParserFactory
     *            the ITupleParserFactory implementation instance.
     * @throws HyracksException
     */
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
        this.executed = new boolean[scheduledLocations.length];
        Arrays.fill(executed, false);
        this.tupleParserFactory = tupleParserFactory;
        this.recordDescriptors[0] = rd;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final InputSplit[] inputSplits = splitsFactory.getSplits();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(ctx.getJobletContext().getClassLoader());
                    JobConf conf = confFactory.getConf();
                    conf.setClassLoader(ctx.getJobletContext().getClassLoader());
                    IKeyValueParser parser = tupleParserFactory.createKeyValueParser(ctx);
                    writer.open();
                    parser.open(writer);
                    InputFormat inputFormat = conf.getInputFormat();
                    for (int i = 0; i < inputSplits.length; i++) {
                        /**
                         * read all the partitions scheduled to the current node
                         */
                        if (scheduledLocations[i].equals(nodeName)) {
                            /**
                             * pick an unread split to read
                             * synchronize among simultaneous partitions in the same machine
                             */
                            synchronized (executed) {
                                if (executed[i] == false) {
                                    executed[i] = true;
                                } else {
                                    continue;
                                }
                            }

                            /**
                             * read the split
                             */
                            RecordReader reader = inputFormat.getRecordReader(inputSplits[i], conf, Reporter.NULL);
                            Object key = reader.createKey();
                            Object value = reader.createValue();
                            while (reader.next(key, value) == true) {
                                parser.parse(key, value, writer, inputSplits[i].toString());
                            }
                        }
                    }
                    parser.close(writer);
                    writer.close();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    Thread.currentThread().setContextClassLoader(ctxCL);
                }
            }
        };
    }
}
