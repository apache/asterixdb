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

package edu.uci.ics.hyracks.hdfs2.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.hdfs.ContextFactory;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

/**
 * The HDFS file read operator using the Hadoop new API. To use this operator, a
 * user need to provide an IKeyValueParserFactory implementation which convert
 * key-value pairs into tuples.
 */
@SuppressWarnings("rawtypes")
public class HDFSReadOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final ConfFactory confFactory;
    private final FileSplitsFactory splitsFactory;
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
     *            the Hadoop JobConf object, which contains the input format and
     *            the input paths
     * @param splits
     *            the array of FileSplits (HDFS chunks).
     * @param scheduledLocations
     *            the node controller names to scan the FileSplits, which is an
     *            one-to-one mapping. The String array is obtained from the
     *            edu.cui
     *            .ics.hyracks.hdfs.scheduler.Scheduler.getLocationConstraints
     *            (InputSplits[]).
     * @param tupleParserFactory
     *            the ITupleParserFactory implementation instance.
     * @throws HyracksException
     */
    public HDFSReadOperatorDescriptor(JobSpecification spec, RecordDescriptor rd, Job conf, List<InputSplit> splits,
            String[] scheduledLocations, IKeyValueParserFactory tupleParserFactory) throws HyracksException {
        super(spec, 0, 1);
        try {
            List<FileSplit> fileSplits = new ArrayList<FileSplit>();
            for (int i = 0; i < splits.size(); i++) {
                fileSplits.add((FileSplit) splits.get(i));
            }
            this.splitsFactory = new FileSplitsFactory(fileSplits);
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
        final List<FileSplit> inputSplits = splitsFactory.getSplits();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
            private ContextFactory ctxFactory = new ContextFactory();

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(ctx.getJobletContext().getClassLoader());
                    Job job = confFactory.getConf();
                    job.getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
                    IKeyValueParser parser = tupleParserFactory.createKeyValueParser(ctx);
                    writer.open();
                    InputFormat inputFormat = ReflectionUtils.newInstance(job.getInputFormatClass(),
                            job.getConfiguration());
                    int size = inputSplits.size();
                    for (int i = 0; i < size; i++) {
                        /**
                         * read all the partitions scheduled to the current node
                         */
                        if (scheduledLocations[i].equals(nodeName)) {
                            /**
                             * pick an unread split to read synchronize among
                             * simultaneous partitions in the same machine
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
                            TaskAttemptContext context = ctxFactory.createContext(job.getConfiguration(), i);
                            context.getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
                            RecordReader reader = inputFormat.createRecordReader(inputSplits.get(i), context);
                            reader.initialize(inputSplits.get(i), context);
                            while (reader.nextKeyValue() == true) {
                                parser.parse(reader.getCurrentKey(), reader.getCurrentValue(), writer,
                                        inputSplits.get(i).toString());
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
