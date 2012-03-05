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

import java.util.Map;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;
import edu.uci.ics.asterix.external.data.parser.IDataParser;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class HiveAdapter extends AbstractDatasourceAdapter implements
		IDatasourceReadAdapter {

	public static final String HIVE_DATABASE = "database";
	public static final String HIVE_TABLE = "table";
	public static final String HIVE_HOME = "hive-home";
	public static final String HIVE_METASTORE_URI = "metastore-uri";
	public static final String HIVE_WAREHOUSE_DIR = "warehouse-dir";
	public static final String HIVE_METASTORE_RAWSTORE_IMPL = "rawstore-impl";

	private HDFSAdapter hdfsAdapter;

	@Override
	public AdapterType getAdapterType() {
		return AdapterType.READ;
	}

	@Override
	public AdapterDataFlowType getAdapterDataFlowType() {
		return AdapterDataFlowType.PULL;
	}

	@Override
	public void configure(Map<String, String> arguments, IAType atype)
			throws Exception {
		configuration = arguments;
		this.atype = atype;
		configureHadoopAdapter();
	}

	private void configureHadoopAdapter() throws Exception {
		String database = configuration.get(HIVE_DATABASE);
		String tablePath = null;
		if (database == null) {
			tablePath = configuration.get(HIVE_WAREHOUSE_DIR) + "/"
					+ configuration.get(HIVE_TABLE);
		} else {
			tablePath = configuration.get(HIVE_WAREHOUSE_DIR) + "/" + tablePath
					+ ".db" + "/" + configuration.get(HIVE_TABLE);
		}
		configuration.put(HDFSAdapter.KEY_HDFS_PATH, tablePath);
		if (!configuration.get(KEY_FORMAT).equals(FORMAT_DELIMITED_TEXT)) {
			throw new IllegalArgumentException("format"
					+ configuration.get(KEY_FORMAT) + " is not supported");
		}

		if (!(configuration.get(HDFSAdapter.KEY_INPUT_FORMAT).equals(
				HDFSAdapter.INPUT_FORMAT_TEXT) || configuration.get(
				HDFSAdapter.KEY_INPUT_FORMAT).equals(
				HDFSAdapter.INPUT_FORMAT_SEQUENCE))) {
			throw new IllegalArgumentException("file input format"
					+ configuration.get(HDFSAdapter.KEY_INPUT_FORMAT)
					+ " is not supported");
		}

		hdfsAdapter = new HDFSAdapter();
		hdfsAdapter.configure(configuration, atype);
	}

	@Override
	public void initialize(IHyracksTaskContext ctx) throws Exception {
		hdfsAdapter.initialize(ctx);
	}

    @Override
    public IDataParser getDataParser(int partition) throws Exception {
        return hdfsAdapter.getDataParser(partition);
    }

}
