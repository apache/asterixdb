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
package edu.uci.ics.asterix.external.data.operator;

import java.util.Map;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ExternalDataScanOperatorDescriptor extends
		AbstractSingleActivityOperatorDescriptor {
	private static final long serialVersionUID = 1L;

	private final String adapter;
	private final Map<String, String> adapterConfiguration;
	private final IAType atype;

	private transient IDatasourceReadAdapter datasourceReadAdapter;

	public ExternalDataScanOperatorDescriptor(JobSpecification spec,
			String adapter, Map<String, String> arguments, IAType atype,
			RecordDescriptor rDesc) {
		super(spec, 0, 1);
		recordDescriptors[0] = rDesc;
		this.adapter = adapter;
		this.adapterConfiguration = arguments;
		this.atype = atype;
	}

	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, final int partition,
			int nPartitions) throws HyracksDataException {

		try {
			datasourceReadAdapter = (IDatasourceReadAdapter) Class.forName(
					adapter).newInstance();
			datasourceReadAdapter.configure(adapterConfiguration, atype);
			datasourceReadAdapter.initialize(ctx);
		} catch (Exception e) {
			throw new HyracksDataException("initialization of adapter failed",
					e);
		}
		return new AbstractUnaryOutputSourceOperatorNodePushable() {
			@Override
			public void initialize() throws HyracksDataException {
				writer.open();
				try {
					datasourceReadAdapter.getDataParser(partition)
							.parse(writer);
				} catch (Exception e) {
					throw new HyracksDataException(
							"exception during reading from external data source",
							e);
				} finally {
					writer.close();
				}
			}
		};
	}
}
