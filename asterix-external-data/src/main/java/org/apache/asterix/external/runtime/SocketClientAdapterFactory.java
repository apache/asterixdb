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
package org.apache.asterix.external.runtime;

import java.util.Map;

import org.apache.asterix.common.feeds.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class SocketClientAdapterFactory implements IAdapterFactory {

    private static final long serialVersionUID = 1L;

    private ARecordType outputType;

    private GenericSocketFeedAdapterFactory genericSocketAdapterFactory;

    private String[] fileSplits;

    public static final String KEY_FILE_SPLITS = "file_splits";

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.outputType = outputType;
        String fileSplitsValue = configuration.get(KEY_FILE_SPLITS);
        if (fileSplitsValue == null) {
            throw new IllegalArgumentException(
                    "File splits not specified. File split is specified as a comma separated list of paths");
        }
        fileSplits = fileSplitsValue.trim().split(",");
        genericSocketAdapterFactory = new GenericSocketFeedAdapterFactory();
        genericSocketAdapterFactory.configure(configuration, outputType);
    }

    @Override
    public String getAlias() {
        return ExternalDataConstants.ALIAS_SOCKET_CLIENT_ADAPTER;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return genericSocketAdapterFactory.getPartitionConstraint();
    }

    @Override
    public IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        Pair<String, Integer> socket = genericSocketAdapterFactory.getSockets().get(partition);
        return new SocketClientAdapter(socket.second, fileSplits[partition]);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

}
