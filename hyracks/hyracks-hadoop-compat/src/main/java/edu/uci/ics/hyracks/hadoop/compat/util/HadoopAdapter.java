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
package edu.uci.ics.hyracks.hadoop.compat.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopMapperOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopReducerOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopHashTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopPartitionerTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.WritableComparingBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.ClasspathBasedHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;

public class HadoopAdapter {

	public static final String FS_DEFAULT_NAME = "fs.default.name";
	private JobConf jobConf;
	private Map<OperatorDescriptorId, Integer> operatorInstanceCount = new HashMap<OperatorDescriptorId, Integer>();
	public static final String HYRACKS_EX_SORT_FRAME_LIMIT = "HYRACKS_EX_SORT_FRAME_LIMIT";
	public static final int DEFAULT_EX_SORT_FRAME_LIMIT = 4096;
	public static final int DEFAULT_MAX_MAPPERS = 40;
	public static final int DEFAULT_MAX_REDUCERS = 40;
	public static final String MAX_MAPPERS_KEY = "maxMappers";
	public static final String MAX_REDUCERS_KEY = "maxReducers";
	public static final String EX_SORT_FRAME_LIMIT_KEY = "sortFrameLimit";

	private int maxMappers = DEFAULT_MAX_MAPPERS;
	private int maxReducers = DEFAULT_MAX_REDUCERS;
	private int exSortFrame = DEFAULT_EX_SORT_FRAME_LIMIT;

	class NewHadoopConstants {
		public static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.inputformat.class";
		public static final String MAP_CLASS_ATTR = "mapreduce.map.class";
		public static final String COMBINE_CLASS_ATTR = "mapreduce.combine.class";
		public static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";
		public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.outputformat.class";
		public static final String PARTITIONER_CLASS_ATTR = "mapreduce.partitioner.class";
	}

	public HadoopAdapter(String namenodeUrl) {
		jobConf = new JobConf(true);
		jobConf.set(FS_DEFAULT_NAME, namenodeUrl);
		if (System.getenv(MAX_MAPPERS_KEY) != null) {
			maxMappers = Integer.parseInt(System.getenv(MAX_MAPPERS_KEY));
		}
		if (System.getenv(MAX_REDUCERS_KEY) != null) {
			maxReducers = Integer.parseInt(System.getenv(MAX_REDUCERS_KEY));
		}
		if (System.getenv(EX_SORT_FRAME_LIMIT_KEY) != null) {
			exSortFrame = Integer.parseInt(System
					.getenv(EX_SORT_FRAME_LIMIT_KEY));
		}
	}

	private String getEnvironmentVariable(String key, String def) {
		String ret = System.getenv(key);
		return ret != null ? ret : def;
	}

	public JobConf getConf() {
		return jobConf;
	}

	public static VersionedProtocol getProtocol(Class protocolClass,
			InetSocketAddress inetAddress, JobConf jobConf) throws IOException {
		VersionedProtocol versionedProtocol = RPC.getProxy(protocolClass,
				ClientProtocol.versionID, inetAddress, jobConf);
		return versionedProtocol;
	}

	private static RecordDescriptor getHadoopRecordDescriptor(
			String className1, String className2) {
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

	private Object[] getInputSplits(JobConf conf) throws IOException,
			ClassNotFoundException, InterruptedException {
		if (conf.getUseNewMapper()) {
			return getNewInputSplits(conf);
		} else {
			return getOldInputSplits(conf);
		}
	}

	private org.apache.hadoop.mapreduce.InputSplit[] getNewInputSplits(
			JobConf conf) throws ClassNotFoundException, IOException,
			InterruptedException {
		org.apache.hadoop.mapreduce.InputSplit[] splits = null;
		JobContext context = new JobContext(conf, null);
		org.apache.hadoop.mapreduce.InputFormat inputFormat = ReflectionUtils
				.newInstance(context.getInputFormatClass(), conf);
		List<org.apache.hadoop.mapreduce.InputSplit> inputSplits = inputFormat
				.getSplits(context);
		return inputSplits
				.toArray(new org.apache.hadoop.mapreduce.InputSplit[] {});
	}

	private InputSplit[] getOldInputSplits(JobConf conf) throws IOException {
		InputFormat inputFormat = conf.getInputFormat();
		return inputFormat.getSplits(conf, conf.getNumMapTasks());
	}

	private void configurePartitionCountConstraint(JobSpecification spec,
			IOperatorDescriptor operator, int instanceCount) {
		PartitionConstraintHelper.addPartitionCountConstraint(spec, operator,
				instanceCount);
		operatorInstanceCount.put(operator.getOperatorId(), instanceCount);
	}

	public HadoopMapperOperatorDescriptor getMapper(JobConf conf,
			JobSpecification spec, IOperatorDescriptor previousOp)
			throws Exception {
		boolean selfRead = previousOp == null;
		IHadoopClassFactory classFactory = new ClasspathBasedHadoopClassFactory();
		HadoopMapperOperatorDescriptor mapOp = null;
		if (selfRead) {
			Object[] splits = getInputSplits(conf, maxMappers);
			mapOp = new HadoopMapperOperatorDescriptor(spec, conf, splits,
					classFactory);
			configurePartitionCountConstraint(spec, mapOp, splits.length);
		} else {
			configurePartitionCountConstraint(spec, mapOp,
					getInstanceCount(previousOp));
			mapOp = new HadoopMapperOperatorDescriptor(spec, conf, classFactory);
			spec.connect(new OneToOneConnectorDescriptor(spec), previousOp, 0,
					mapOp, 0);
		}
		return mapOp;
	}

	public HadoopReducerOperatorDescriptor getReducer(JobConf conf,
			IOperatorDescriptorRegistry spec, boolean useAsCombiner) {
		HadoopReducerOperatorDescriptor reduceOp = new HadoopReducerOperatorDescriptor(
				spec, conf, null, new ClasspathBasedHadoopClassFactory(),
				useAsCombiner);
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

	public JobSpecification getJobSpecification(List<JobConf> jobConfs)
			throws Exception {
		JobSpecification spec = null;
		if (jobConfs.size() == 1) {
			spec = getJobSpecification(jobConfs.get(0));
		} else {
			spec = getPipelinedSpec(jobConfs);
		}
		return spec;
	}

	private IOperatorDescriptor configureOutput(
			IOperatorDescriptor previousOperator, JobConf conf,
			JobSpecification spec) throws Exception {
		int instanceCountPreviousOperator = operatorInstanceCount
				.get(previousOperator.getOperatorId());
		int numOutputters = conf.getNumReduceTasks() != 0 ? conf
				.getNumReduceTasks() : instanceCountPreviousOperator;
		HadoopWriteOperatorDescriptor writer = null;
		writer = new HadoopWriteOperatorDescriptor(spec, conf, numOutputters);
		configurePartitionCountConstraint(spec, writer, numOutputters);
		spec.connect(new OneToOneConnectorDescriptor(spec), previousOperator,
				0, writer, 0);
		return writer;
	}

	private int getInstanceCount(IOperatorDescriptor operator) {
		return operatorInstanceCount.get(operator.getOperatorId());
	}

	private IOperatorDescriptor addCombiner(
			IOperatorDescriptor previousOperator, JobConf jobConf,
			JobSpecification spec) throws Exception {
		boolean useCombiner = (jobConf.getCombinerClass() != null);
		IOperatorDescriptor mapSideOutputOp = previousOperator;
		if (useCombiner) {
			System.out.println("Using Combiner:"
					+ jobConf.getCombinerClass().getName());
			IOperatorDescriptor mapSideCombineSortOp = getExternalSorter(
					jobConf, spec);
			configurePartitionCountConstraint(spec, mapSideCombineSortOp,
					getInstanceCount(previousOperator));

			HadoopReducerOperatorDescriptor mapSideCombineReduceOp = getReducer(
					jobConf, spec, true);
			configurePartitionCountConstraint(spec, mapSideCombineReduceOp,
					getInstanceCount(previousOperator));
			spec.connect(new OneToOneConnectorDescriptor(spec),
					previousOperator, 0, mapSideCombineSortOp, 0);
			spec.connect(new OneToOneConnectorDescriptor(spec),
					mapSideCombineSortOp, 0, mapSideCombineReduceOp, 0);
			mapSideOutputOp = mapSideCombineReduceOp;
		}
		return mapSideOutputOp;
	}

	private int getNumReduceTasks(JobConf jobConf) {
		int numReduceTasks = Math.min(maxReducers, jobConf.getNumReduceTasks());
		return numReduceTasks;
	}

	private IOperatorDescriptor addReducer(
			IOperatorDescriptor previousOperator, JobConf jobConf,
			JobSpecification spec) throws Exception {
		IOperatorDescriptor mrOutputOperator = previousOperator;
		if (jobConf.getNumReduceTasks() != 0) {
			IOperatorDescriptor sorter = getExternalSorter(jobConf, spec);
			HadoopReducerOperatorDescriptor reducer = getReducer(jobConf, spec,
					false);
			int numReduceTasks = getNumReduceTasks(jobConf);
			configurePartitionCountConstraint(spec, sorter, numReduceTasks);
			configurePartitionCountConstraint(spec, reducer, numReduceTasks);

			IConnectorDescriptor mToNConnectorDescriptor = getMtoNHashPartitioningConnector(
					jobConf, spec);
			spec.connect(mToNConnectorDescriptor, previousOperator, 0, sorter,
					0);
			spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0,
					reducer, 0);
			mrOutputOperator = reducer;
		}
		return mrOutputOperator;
	}

	private long getInputSize(Object[] splits, JobConf conf)
			throws IOException, InterruptedException {
		long totalInputSize = 0;
		if (conf.getUseNewMapper()) {
			for (org.apache.hadoop.mapreduce.InputSplit split : (org.apache.hadoop.mapreduce.InputSplit[]) splits) {
				totalInputSize += split.getLength();
			}
		} else {
			for (InputSplit split : (InputSplit[]) splits) {
				totalInputSize += split.getLength();
			}
		}
		return totalInputSize;
	}

	private Object[] getInputSplits(JobConf conf, int desiredMaxMappers)
			throws Exception {
		Object[] splits = getInputSplits(conf);
		if (splits.length > desiredMaxMappers) {
			long totalInputSize = getInputSize(splits, conf);
			long goalSize = (totalInputSize / desiredMaxMappers);
			conf.setLong("mapred.min.split.size", goalSize);
			conf.setNumMapTasks(desiredMaxMappers);
			splits = getInputSplits(conf);
		}
		return splits;
	}

	public JobSpecification getPipelinedSpec(List<JobConf> jobConfs)
			throws Exception {
		JobSpecification spec = new JobSpecification();
		Iterator<JobConf> iterator = jobConfs.iterator();
		JobConf firstMR = iterator.next();
		IOperatorDescriptor mrOutputOp = configureMapReduce(null, spec, firstMR);
		while (iterator.hasNext())
			for (JobConf currentJobConf : jobConfs) {
				mrOutputOp = configureMapReduce(mrOutputOp, spec,
						currentJobConf);
			}
		configureOutput(mrOutputOp, jobConfs.get(jobConfs.size() - 1), spec);
		return spec;
	}

	public JobSpecification getJobSpecification(JobConf conf) throws Exception {
		JobSpecification spec = new JobSpecification();
		IOperatorDescriptor mrOutput = configureMapReduce(null, spec, conf);
		IOperatorDescriptor printer = configureOutput(mrOutput, conf, spec);
		spec.addRoot(printer);
		System.out.println(spec);
		return spec;
	}

	private IOperatorDescriptor configureMapReduce(
			IOperatorDescriptor previousOuputOp, JobSpecification spec,
			JobConf conf) throws Exception {
		IOperatorDescriptor mapper = getMapper(conf, spec, previousOuputOp);
		IOperatorDescriptor mapSideOutputOp = addCombiner(mapper, conf, spec);
		IOperatorDescriptor reducer = addReducer(mapSideOutputOp, conf, spec);
		return reducer;
	}

	public static InMemorySortOperatorDescriptor getInMemorySorter(
			JobConf conf, IOperatorDescriptorRegistry spec) {
		InMemorySortOperatorDescriptor inMemorySortOp = null;
		RecordDescriptor recordDescriptor = getHadoopRecordDescriptor(conf
				.getMapOutputKeyClass().getName(), conf
				.getMapOutputValueClass().getName());
		Class<? extends RawComparator> rawComparatorClass = null;
		WritableComparator writableComparator = WritableComparator.get(conf
				.getMapOutputKeyClass().asSubclass(WritableComparable.class));
		WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(
				writableComparator.getClass());
		inMemorySortOp = new InMemorySortOperatorDescriptor(spec,
				new int[] { 0 },
				new IBinaryComparatorFactory[] { comparatorFactory },
				recordDescriptor);
		return inMemorySortOp;
	}

	public static ExternalSortOperatorDescriptor getExternalSorter(
			JobConf conf, IOperatorDescriptorRegistry spec) {
		ExternalSortOperatorDescriptor externalSortOp = null;
		RecordDescriptor recordDescriptor = getHadoopRecordDescriptor(conf
				.getMapOutputKeyClass().getName(), conf
				.getMapOutputValueClass().getName());
		Class<? extends RawComparator> rawComparatorClass = null;
		WritableComparator writableComparator = WritableComparator.get(conf
				.getMapOutputKeyClass().asSubclass(WritableComparable.class));
		WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(
				writableComparator.getClass());
		externalSortOp = new ExternalSortOperatorDescriptor(spec, conf.getInt(
				HYRACKS_EX_SORT_FRAME_LIMIT, DEFAULT_EX_SORT_FRAME_LIMIT),
				new int[] { 0 },
				new IBinaryComparatorFactory[] { comparatorFactory },
				recordDescriptor);
		return externalSortOp;
	}

	public static MToNPartitioningConnectorDescriptor getMtoNHashPartitioningConnector(
			JobConf conf, IConnectorDescriptorRegistry spec) {

		Class mapOutputKeyClass = conf.getMapOutputKeyClass();
		Class mapOutputValueClass = conf.getMapOutputValueClass();

		MToNPartitioningConnectorDescriptor connectorDescriptor = null;
		ITuplePartitionComputerFactory factory = null;
		conf.getMapOutputKeyClass();
		if (conf.getPartitionerClass() != null
				&& !conf.getPartitionerClass().getName().startsWith(
						"org.apache.hadoop")) {
			Class<? extends Partitioner> partitioner = conf
					.getPartitionerClass();
			factory = new HadoopPartitionerTuplePartitionComputerFactory(
					partitioner, DatatypeHelper
							.createSerializerDeserializer(mapOutputKeyClass),
					DatatypeHelper
							.createSerializerDeserializer(mapOutputValueClass));
		} else {
			RecordDescriptor recordDescriptor = DatatypeHelper
					.createKeyValueRecordDescriptor(mapOutputKeyClass,
							mapOutputValueClass);
			ISerializerDeserializer mapOutputKeySerializerDerserializer = DatatypeHelper
					.createSerializerDeserializer(mapOutputKeyClass);
			factory = new HadoopHashTuplePartitionComputerFactory(
					mapOutputKeySerializerDerserializer);
		}
		connectorDescriptor = new MToNPartitioningConnectorDescriptor(spec,
				factory);
		return connectorDescriptor;
	}

}
