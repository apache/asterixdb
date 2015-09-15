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
package org.apache.asterix.metadata.declared;

import java.util.Map;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.metadata.external.IAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

/**
 *
 * @author alamouda
 * This wrapper adapter factory is an adapter that is used when loading data into a dataset with a UUID primary key. The factory creates an adapter that
 * generates UUIDs and merge them into parsed records
 */
public class PKGeneratingAdapterFactory implements IAdapterFactory {

    private static final long serialVersionUID = 1L;
    private final IAdapterFactory wrappedAdapterFactory;
    private final RecordDescriptor inRecDesc;
    private final RecordDescriptor outRecDesc;
    private final ARecordType inRecType;
    private final ARecordType outRecType;
    private final int pkIndex;

    public PKGeneratingAdapterFactory(IAdapterFactory wrappedAdapterFactory, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, ARecordType inRecType, ARecordType outRecType, int pkIndex) {
        this.wrappedAdapterFactory = wrappedAdapterFactory;
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.inRecType = inRecType;
        this.outRecType = outRecType;
        this.pkIndex = pkIndex;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return wrappedAdapterFactory.getSupportedOperations();
    }

    @Override
    public String getName() {
        return "PKGeneratingAdapter[ " + wrappedAdapterFactory.getName() + " ]";
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return wrappedAdapterFactory.getPartitionConstraint();
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        IDatasourceAdapter wrappedAdapter = wrappedAdapterFactory.createAdapter(ctx, partition);
        return new PKGeneratingAdapter(ctx, inRecDesc, outRecDesc, inRecType, outRecType, wrappedAdapter, pkIndex);
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        wrappedAdapterFactory.configure(configuration, outputType);        
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return wrappedAdapterFactory.getAdapterOutputType();
    }
}
