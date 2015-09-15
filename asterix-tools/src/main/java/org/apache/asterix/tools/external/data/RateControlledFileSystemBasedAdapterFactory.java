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
package org.apache.asterix.tools.external.data;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.external.adapter.factory.HDFSAdapterFactory;
import org.apache.asterix.external.adapter.factory.NCFileSystemAdapterFactory;
import org.apache.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import org.apache.asterix.metadata.external.IAdapterFactory;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{RateControllerFileSystemBasedAdapter} The
 * adapter simulates a feed from the contents of a source file. The file can be
 * on the local file system or on HDFS. The feed ends when the content of the
 * source file has been ingested.
 */
public class RateControlledFileSystemBasedAdapterFactory extends StreamBasedAdapterFactory implements
        IFeedAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String KEY_FILE_SYSTEM = "fs";
    public static final String LOCAL_FS = "localfs";
    public static final String HDFS = "hdfs";
    public static final String KEY_PATH = "path";
    public static final String KEY_FORMAT = "format";

    private IAdapterFactory adapterFactory;
    private String format;
    private ARecordType atype;

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        FileSystemBasedAdapter coreAdapter = (FileSystemBasedAdapter) adapterFactory.createAdapter(ctx, partition);
        return new RateControlledFileSystemBasedAdapter(atype, configuration, coreAdapter, format, parserFactory, ctx);
    }

    @Override
    public String getName() {
        return "file_feed";
    }

    private void checkRequiredArgs(Map<String, String> configuration) throws Exception {
        if (configuration.get(KEY_FILE_SYSTEM) == null) {
            throw new Exception("File system type not specified. (fs=?) File system could be 'localfs' or 'hdfs'");
        }
        if (configuration.get(IAdapterFactory.KEY_TYPE_NAME) == null) {
            throw new Exception("Record type not specified (type-name=?)");
        }
        if (configuration.get(KEY_PATH) == null) {
            throw new Exception("File path not specified (path=?)");
        }
        if (configuration.get(KEY_FORMAT) == null) {
            throw new Exception("File format not specified (format=?)");
        }
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        checkRequiredArgs(configuration);
        String fileSystem = (String) configuration.get(KEY_FILE_SYSTEM);
        String adapterFactoryClass = null;
        if (fileSystem.equalsIgnoreCase(LOCAL_FS)) {
            adapterFactoryClass = NCFileSystemAdapterFactory.class.getName();
        } else if (fileSystem.equals(HDFS)) {
            adapterFactoryClass = HDFSAdapterFactory.class.getName();
        } else {
            throw new AsterixException("Unsupported file system type " + fileSystem);
        }
        this.atype = outputType;
        format = configuration.get(KEY_FORMAT);
        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClass).newInstance();
        adapterFactory.configure(configuration, outputType);
        configureFormat(outputType);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return adapterFactory.getPartitionConstraint();
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return atype;
    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.UNKNOWN;
    }

    public boolean isRecordTrackingEnabled() {
        return false;
    }

    public IIntakeProgressTracker createIntakeProgressTracker() {
        throw new UnsupportedOperationException("Tracking of ingested records not enabled");
    }

}
