/*
 * Copyright 2009-2010 University of California, Irvine
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
package edu.uci.ics.hyracks.dataflow.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.constraints.PartitionCountConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.InputSplitHandler;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.file.IRecordReader;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class HadoopReadOperatorDescriptor extends
		AbstractSingleActivityOperatorDescriptor {

	protected transient InputSplit[] splits;
	protected transient Path splitFilePath;
	protected transient JobConf jobConf;
	protected transient Reporter reporter;

	private static final long serialVersionUID = 1L;
	private String inputFormatClassName;
	private Map<String, String> jobConfMap;
	private static String splitDirectory = "/tmp/splits";

	private void initialize() {
		try {
			reporter = createReporter();
			if (jobConf == null) {
				jobConf = DatatypeHelper.map2JobConf(jobConfMap);
			}
			splits = InputSplitHandler.getInputSplits(jobConf, splitFilePath);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	protected class FileScanOperator implements IOpenableDataWriterOperator {
		private IOpenableDataWriter<Object[]> writer;

		private int index;

		FileScanOperator(int index) {
			this.index = index;
		}

		@Override
		public void setDataWriter(int index,
				IOpenableDataWriter<Object[]> writer) {
			if (index != 0) {
				throw new IndexOutOfBoundsException("Invalid index: " + index);
			}
			this.writer = writer;
		}

		@Override
		public void open() throws HyracksDataException {
			if (splits == null) {
				// initialize splits by reading from split file
				initialize();
			}
			InputSplit split = splits[index];
			RecordDescriptor desc = recordDescriptors[0];
			try {
				IRecordReader reader = createRecordReader(split, desc);
				if (desc == null) {
					desc = recordDescriptors[0];
				}
				writer.open();
				try {
					while (true) {
						Object[] record = new Object[desc.getFields().length];
						if (!reader.read(record)) {
							break;
						}
						writer.writeData(record);
					}
				} finally {
					reader.close();
					writer.close();
					splitFilePath = null;
				}
			} catch (Exception e) {
				throw new HyracksDataException(e);
			}
		}

		@Override
		public void close() throws HyracksDataException {
			// do nothing
			splitFilePath = null;

		}

		@Override
		public void writeData(Object[] data) throws HyracksDataException {
			throw new UnsupportedOperationException();
		}
	}

	protected class HDFSCustomReader implements IRecordReader {
		private RecordReader hadoopRecordReader;
		private Object key;
		private Object value;
		private FileSystem fileSystem;

		public HDFSCustomReader(Map<String, String> jobConfMap,
				InputSplit inputSplit, String inputFormatClassName,
				Reporter reporter) {
			try {
				JobConf conf = DatatypeHelper.map2JobConf((HashMap) jobConfMap);
				try {
					fileSystem = FileSystem.get(conf);
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}

				Class inputFormatClass = Class.forName(inputFormatClassName);
				InputFormat inputFormat = (InputFormat) ReflectionUtils
						.newInstance(inputFormatClass, conf);
				hadoopRecordReader = (RecordReader) inputFormat
						.getRecordReader(inputSplit, conf, reporter);
				Class inputKeyClass;
				Class inputValueClass;
				if (hadoopRecordReader instanceof SequenceFileRecordReader) {
					inputKeyClass = ((SequenceFileRecordReader) hadoopRecordReader)
							.getKeyClass();
					inputValueClass = ((SequenceFileRecordReader) hadoopRecordReader)
							.getValueClass();
				} else {
					inputKeyClass = hadoopRecordReader.createKey().getClass();
					inputValueClass = hadoopRecordReader.createValue()
							.getClass();
				}
				key = inputKeyClass.newInstance();
				value = inputValueClass.newInstance();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void close() {
			try {
				hadoopRecordReader.close();
				if (fileSystem != null) {
					fileSystem.delete(splitFilePath);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public boolean read(Object[] record) throws Exception {
			if (!hadoopRecordReader.next(key, value)) {
				return false;
			}
			if (record.length == 1) {
				record[0] = value;
			} else {
				record[0] = key;
				record[1] = value;
			}
			return true;
		}
	}

	public HadoopReadOperatorDescriptor(JobConf jobConf, JobSpecification spec)
			throws IOException {
		super(spec, 0, 1);
		this.jobConfMap = DatatypeHelper.jobConf2Map(jobConf);
		InputFormat inputFormat = jobConf.getInputFormat();
		InputSplit[] splits = inputFormat.getSplits(jobConf, jobConf
				.getNumMapTasks());
		RecordReader recordReader = inputFormat.getRecordReader(splits[0],
				jobConf, createReporter());
		super.recordDescriptors[0] = DatatypeHelper
				.createKeyValueRecordDescriptor(
						(Class<? extends Writable>) recordReader.createKey()
								.getClass(),
						(Class<? extends Writable>) recordReader.createValue()
								.getClass());
		String suffix = "" + System.currentTimeMillis();
		splitFilePath = new Path(splitDirectory, suffix);
		InputSplitHandler.writeSplitFile(splits, jobConf, splitFilePath);
		this
				.setPartitionConstraint(new PartitionCountConstraint(
						splits.length));
		this.inputFormatClassName = inputFormat.getClass().getName();
	}

	protected IRecordReader createRecordReader(InputSplit fileSplit,
			RecordDescriptor desc) throws Exception {
		IRecordReader recordReader = new HDFSCustomReader(jobConfMap,
				fileSplit, inputFormatClassName, reporter);
		return recordReader;
	}

	protected Reporter createReporter() {
		return new Reporter() {
			@Override
			public Counter getCounter(Enum<?> name) {
				return null;
			}

			@Override
			public Counter getCounter(String group, String name) {
				return null;
			}

			@Override
			public InputSplit getInputSplit()
					throws UnsupportedOperationException {
				return null;
			}

			@Override
			public void incrCounter(Enum<?> key, long amount) {

			}

			@Override
			public void incrCounter(String group, String counter, long amount) {

			}

			@Override
			public void progress() {

			}

			@Override
			public void setStatus(String status) {

			}
		};
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksContext ctx,
			IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) {
		return new DeserializedOperatorNodePushable(ctx, new FileScanOperator(
				partition), null);
	}

}
