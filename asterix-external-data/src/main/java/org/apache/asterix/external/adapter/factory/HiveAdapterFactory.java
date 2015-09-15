/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.adapter.factory;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.external.dataset.adapter.HDFSAdapter;
import org.apache.asterix.external.dataset.adapter.HiveAdapter;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.external.IAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * A factory class for creating an instance of HiveAdapter
 */
public class HiveAdapterFactory extends StreamBasedAdapterFactory implements IAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String HIVE_DATABASE = "database";
    public static final String HIVE_TABLE = "table";
    public static final String HIVE_HOME = "hive-home";
    public static final String HIVE_METASTORE_URI = "metastore-uri";
    public static final String HIVE_WAREHOUSE_DIR = "warehouse-dir";
    public static final String HIVE_METASTORE_RAWSTORE_IMPL = "rawstore-impl";

    private HDFSAdapterFactory hdfsAdapterFactory;
    private HDFSAdapter hdfsAdapter;
    private boolean configured = false;
    private IAType atype;

    public HiveAdapterFactory() {
        hdfsAdapterFactory = new HDFSAdapterFactory();
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        hdfsAdapter = (HDFSAdapter) hdfsAdapterFactory.createAdapter(ctx, partition);
        HiveAdapter hiveAdapter = new HiveAdapter(atype, hdfsAdapter, parserFactory, ctx);
        return hiveAdapter;
    }

    @Override
    public String getName() {
        return "hive";
    }

  
    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        if (!configured) {
            populateConfiguration(configuration);
            hdfsAdapterFactory.configure(configuration, outputType);
            this.atype = (IAType) outputType;
        }
    }

    public static void populateConfiguration(Map<String, String> configuration) throws Exception {
        /** configure hive */
        String database = (String) configuration.get(HIVE_DATABASE);
        String tablePath = null;
        if (database == null) {
            tablePath = configuration.get(HIVE_WAREHOUSE_DIR) + "/" + configuration.get(HIVE_TABLE);
        } else {
            tablePath = configuration.get(HIVE_WAREHOUSE_DIR) + "/" + tablePath + ".db" + "/"
                    + configuration.get(HIVE_TABLE);
        }
        configuration.put(HDFSAdapterFactory.KEY_PATH, tablePath);
        if (!configuration.get(AsterixTupleParserFactory.KEY_FORMAT).equals(AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT)) {
            throw new IllegalArgumentException("format" + configuration.get(AsterixTupleParserFactory.KEY_FORMAT) + " is not supported");
        }

        if (!(configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT) || configuration
                .get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_SEQUENCE))) {
            throw new IllegalArgumentException("file input format"
                    + configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT) + " is not supported");
        }
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return hdfsAdapterFactory.getPartitionConstraint();
    }

    
    @Override
    public ARecordType getAdapterOutputType() {
        return (ARecordType) atype;
    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.UNKNOWN;
    }

    public void setFiles(List<ExternalFile> files) {
        hdfsAdapterFactory.setFiles(files);
    }

}
