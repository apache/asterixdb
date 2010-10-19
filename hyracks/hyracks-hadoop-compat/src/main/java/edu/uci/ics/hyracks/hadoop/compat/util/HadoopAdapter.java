/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.hadoop.compat.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionCountConstraint;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopMapperOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopReadOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopReducerOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopHashTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopPartitionerTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.WritableComparingBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.ClasspathBasedHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;

public class HadoopAdapter {

    public static final String FS_DEFAULT_NAME = "fs.default.name";
    private JobConf jobConf;

    public HadoopAdapter(String namenodeUrl) {
        jobConf = new JobConf(true);
        jobConf.set(FS_DEFAULT_NAME, namenodeUrl);
    }

    public JobConf getConf() {
        return jobConf;
    }

    public static VersionedProtocol getProtocol(Class protocolClass, InetSocketAddress inetAddress, JobConf jobConf)
            throws IOException {
        VersionedProtocol versionedProtocol = RPC.getProxy(protocolClass, ClientProtocol.versionID, inetAddress,
                jobConf);
        return versionedProtocol;
    }

    private static RecordDescriptor getHadoopRecordDescriptor(String className1, String className2) {
        RecordDescriptor recordDescriptor = null;
        try {
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                    (Class<? extends Writable>) Class.forName(className1),
                    (Class<? extends Writable>) Class.forName(className2));
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }
        return recordDescriptor;
    }

    private InputSplit[] getInputSplits(JobConf jobConf) throws IOException {
        InputFormat inputFormat = jobConf.getInputFormat();
        return inputFormat.getSplits(jobConf, jobConf.getNumMapTasks());
    }

    public HadoopMapperOperatorDescriptor getMapper(JobConf conf, InputSplit[] splits, JobSpecification spec)
            throws IOException {
        HadoopMapperOperatorDescriptor mapOp = new HadoopMapperOperatorDescriptor(spec, conf, splits,
                new ClasspathBasedHadoopClassFactory());
        return mapOp;
    }

    public HadoopReducerOperatorDescriptor getReducer(JobConf conf, JobSpecification spec) {
        HadoopReducerOperatorDescriptor reduceOp = new HadoopReducerOperatorDescriptor(spec, conf, null,
                new ClasspathBasedHadoopClassFactory());
        return reduceOp;
    }

    public FileSystem getHDFSClient() {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(jobConf);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return fileSystem;
    }

    public JobSpecification getJobSpecification(List<JobConf> jobConfs) throws Exception {
        JobSpecification spec = null;
        if (jobConfs.size() == 1) {
            spec = getJobSpecification(jobConfs.get(0));
        } else {
            spec = getPipelinedSpec(jobConfs);
        }
        return spec;
    }

    private IOperatorDescriptor configureOutput(IOperatorDescriptor previousOperator, JobConf conf,
            JobSpecification spec) throws Exception {
        PartitionConstraint previousOpConstraint = previousOperator.getPartitionConstraint();
        int noOfInputs = previousOpConstraint instanceof PartitionCountConstraint ? ((PartitionCountConstraint) previousOpConstraint)
                .getCount() : ((ExplicitPartitionConstraint) previousOpConstraint).getLocationConstraints().length;
        int numOutputters = conf.getNumReduceTasks() != 0 ? conf.getNumReduceTasks() : noOfInputs;
        HadoopWriteOperatorDescriptor writer = null;
        writer = new HadoopWriteOperatorDescriptor(spec, conf, numOutputters);
        writer.setPartitionConstraint(previousOperator.getPartitionConstraint());
        spec.connect(new OneToOneConnectorDescriptor(spec), previousOperator, 0, writer, 0);
        return writer;
    }

    private IOperatorDescriptor addMRToExistingPipeline(IOperatorDescriptor previousOperator, JobConf jobConf,
            JobSpecification spec, InputSplit[] splits) throws IOException {
        HadoopMapperOperatorDescriptor mapOp = getMapper(jobConf, splits, spec);
        IOperatorDescriptor mrOutputOperator = mapOp;
        PartitionConstraint mapperPartitionConstraint = previousOperator.getPartitionConstraint();
        mapOp.setPartitionConstraint(mapperPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), previousOperator, 0, mapOp, 0);
        IOperatorDescriptor mapOutputOperator = mapOp;

        boolean useCombiner = (jobConf.getCombinerClass() != null);
        if (useCombiner) {
            System.out.println("Using Combiner:" + jobConf.getCombinerClass().getName());
            InMemorySortOperatorDescriptor mapSideCombineSortOp = getSorter(jobConf, spec);
            mapSideCombineSortOp.setPartitionConstraint(mapperPartitionConstraint);

            HadoopReducerOperatorDescriptor mapSideCombineReduceOp = getReducer(jobConf, spec);
            mapSideCombineReduceOp.setPartitionConstraint(mapperPartitionConstraint);
            mapOutputOperator = mapSideCombineReduceOp;

            spec.connect(new OneToOneConnectorDescriptor(spec), mapOp, 0, mapSideCombineSortOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), mapSideCombineSortOp, 0, mapSideCombineReduceOp, 0);
            mrOutputOperator = mapSideCombineReduceOp;
        }

        if (jobConf.getNumReduceTasks() != 0) {
            IOperatorDescriptor sorter = getSorter(jobConf, spec);
            HadoopReducerOperatorDescriptor reducer = getReducer(jobConf, spec);

            PartitionConstraint reducerPartitionConstraint = new PartitionCountConstraint(jobConf.getNumReduceTasks());
            sorter.setPartitionConstraint(reducerPartitionConstraint);
            reducer.setPartitionConstraint(reducerPartitionConstraint);

            IConnectorDescriptor mToNConnectorDescriptor = getMtoNHashPartitioningConnector(jobConf, spec);
            spec.connect(mToNConnectorDescriptor, mapOutputOperator, 0, sorter, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, reducer, 0);
            mrOutputOperator = reducer;
        }
        return mrOutputOperator;
    }

    public JobSpecification getPipelinedSpec(List<JobConf> jobConfs) throws Exception {
        JobSpecification spec = new JobSpecification();
        Iterator<JobConf> iterator = jobConfs.iterator();
        JobConf firstMR = iterator.next();
        InputSplit[] splits = getInputSplits(firstMR);
        IOperatorDescriptor reader = new HadoopReadOperatorDescriptor(firstMR, spec, splits);
        IOperatorDescriptor outputOperator = reader;
        outputOperator = addMRToExistingPipeline(reader, firstMR, spec, splits);
        while (iterator.hasNext())
            for (JobConf currentJobConf : jobConfs) {
                outputOperator = addMRToExistingPipeline(outputOperator, currentJobConf, spec, null);
            }
        configureOutput(outputOperator, jobConfs.get(jobConfs.size() - 1), spec);
        return spec;
    }

    public JobSpecification getJobSpecification(JobConf conf) throws Exception {
        JobSpecification spec = new JobSpecification();
        InputSplit[] splits = getInputSplits(conf);
        HadoopReadOperatorDescriptor hadoopReadOperatorDescriptor = new HadoopReadOperatorDescriptor(conf, spec, splits);
        IOperatorDescriptor mrOutputOperator = addMRToExistingPipeline(hadoopReadOperatorDescriptor, conf, spec, splits);
        IOperatorDescriptor printer = configureOutput(mrOutputOperator, conf, spec);
        spec.addRoot(printer);
        return spec;
    }

    public static InMemorySortOperatorDescriptor getSorter(JobConf conf, JobSpecification spec) {
        InMemorySortOperatorDescriptor inMemorySortOp = null;
        RecordDescriptor recordDescriptor = getHadoopRecordDescriptor(conf.getMapOutputKeyClass().getName(), conf
                .getMapOutputValueClass().getName());
        Class<? extends RawComparator> rawComparatorClass = null;
        WritableComparator writableComparator = WritableComparator.get(conf.getMapOutputKeyClass().asSubclass(
                WritableComparable.class));
        WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(
                writableComparator.getClass());
        inMemorySortOp = new InMemorySortOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { comparatorFactory }, recordDescriptor);
        return inMemorySortOp;
    }

    public static MToNHashPartitioningConnectorDescriptor getMtoNHashPartitioningConnector(JobConf conf,
            JobSpecification spec) {

        Class mapOutputKeyClass = conf.getMapOutputKeyClass();
        Class mapOutputValueClass = conf.getMapOutputValueClass();

        MToNHashPartitioningConnectorDescriptor connectorDescriptor = null;
        ITuplePartitionComputerFactory factory = null;
        conf.getMapOutputKeyClass();
        if (conf.getPartitionerClass() != null && !conf.getPartitionerClass().getName().startsWith("org.apache.hadoop")) {
            Class<? extends Partitioner> partitioner = conf.getPartitionerClass();
            factory = new HadoopPartitionerTuplePartitionComputerFactory(partitioner,
                    DatatypeHelper.createSerializerDeserializer(mapOutputKeyClass),
                    DatatypeHelper.createSerializerDeserializer(mapOutputValueClass));
        } else {
            RecordDescriptor recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(mapOutputKeyClass,
                    mapOutputValueClass);
            ISerializerDeserializer mapOutputKeySerializerDerserializer = DatatypeHelper
                    .createSerializerDeserializer(mapOutputKeyClass);
            factory = new HadoopHashTuplePartitionComputerFactory(mapOutputKeySerializerDerserializer);
        }
        connectorDescriptor = new MToNHashPartitioningConnectorDescriptor(spec, factory);
        return connectorDescriptor;
    }

}
