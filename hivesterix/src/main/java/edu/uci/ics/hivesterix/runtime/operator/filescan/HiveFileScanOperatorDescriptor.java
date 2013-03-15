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
package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.mapred.FileSplit;

import edu.uci.ics.hivesterix.runtime.config.ConfUtil;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

@SuppressWarnings("deprecation")
public class HiveFileScanOperatorDescriptor extends
		AbstractSingleActivityOperatorDescriptor {
	private static final long serialVersionUID = 1L;

	/**
	 * tuple parser factory
	 */
	private final ITupleParserFactory tupleParserFactory;

	/**
	 * Hive file split
	 */
	private Partition[] parts;

	/**
	 * IFileSplitProvider
	 */
	private IFileSplitProvider fileSplitProvider;

	/**
	 * constrains in the form of host DNS names
	 */
	private String[] constraintsByHostNames;

	/**
	 * ip-to-node controller mapping
	 */
	private Map<String, List<String>> ncMapping;

	/**
	 * an array of NCs
	 */
	private String[] NCs;

	/**
	 * 
	 * @param spec
	 * @param fsProvider
	 */
	public HiveFileScanOperatorDescriptor(JobSpecification spec,
			IFileSplitProvider fsProvider,
			ITupleParserFactory tupleParserFactory, RecordDescriptor rDesc) {
		super(spec, 0, 1);
		this.tupleParserFactory = tupleParserFactory;
		recordDescriptors[0] = rDesc;
		fileSplitProvider = fsProvider;
	}

	/**
	 * set partition constraint at the first time it is called the number of
	 * partitions is obtained from HDFS name node
	 */
	public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
			throws AlgebricksException {
		FileSplit[] returnedSplits = ((AbstractHiveFileSplitProvider) fileSplitProvider)
				.getFileSplitArray();
		Random random = new Random(System.currentTimeMillis());
		ncMapping = ConfUtil.getNCMapping();
		NCs = ConfUtil.getNCs();

		int size = 0;
		for (FileSplit split : returnedSplits)
			if (split != null)
				size++;

		FileSplit[] splits = new FileSplit[size];
		for (int i = 0; i < returnedSplits.length; i++)
			if (returnedSplits[i] != null)
				splits[i] = returnedSplits[i];

		System.out.println("number of splits: " + splits.length);
		constraintsByHostNames = new String[splits.length];
		for (int i = 0; i < splits.length; i++) {
			try {
				String[] loc = splits[i].getLocations();
				Collections.shuffle(Arrays.asList(loc), random);
				if (loc.length > 0) {
					InetAddress[] allIps = InetAddress.getAllByName(loc[0]);
					for (InetAddress ip : allIps) {
						if (ncMapping.get(ip.getHostAddress()) != null) {
							List<String> ncs = ncMapping.get(ip
									.getHostAddress());
							int pos = random.nextInt(ncs.size());
							constraintsByHostNames[i] = ncs.get(pos);
						} else {
							int pos = random.nextInt(NCs.length);
							constraintsByHostNames[i] = NCs[pos];
						}
					}
				} else {
					int pos = random.nextInt(NCs.length);
					constraintsByHostNames[i] = NCs[pos];
					if (splits[i].getLength() > 0)
						throw new IllegalStateException(
								"non local scanner non locations!!");
				}
			} catch (IOException e) {
				throw new AlgebricksException(e);
			}
		}

		parts = new Partition[splits.length];
		for (int i = 0; i < splits.length; i++) {
			parts[i] = new Partition(splits[i]);
		}
		return new AlgebricksAbsolutePartitionConstraint(constraintsByHostNames);
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) {

		final ITupleParser tp = tupleParserFactory.createTupleParser(ctx);
		final int partitionId = partition;

		return new AbstractUnaryOutputSourceOperatorNodePushable() {

			@Override
			public void initialize() throws HyracksDataException {
				writer.open();
				FileSplit split = parts[partitionId].toFileSplit();
				if (split == null)
					throw new HyracksDataException("partition " + partitionId
							+ " is null!");
				((AbstractHiveTupleParser) tp).parse(split, writer);
				writer.close();
			}
		};
	}
}