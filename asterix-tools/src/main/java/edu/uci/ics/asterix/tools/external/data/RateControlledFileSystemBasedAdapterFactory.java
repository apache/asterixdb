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
package edu.uci.ics.asterix.tools.external.data;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.adapter.factory.IGenericDatasetAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * Factory class for creating @see{RateControllerFileSystemBasedAdapter} The
 * adapter simulates a feed from the contents of a source file. The file can be
 * on the local file system or on HDFS. The feed ends when the content of the
 * source file has been ingested.
 */
public class RateControlledFileSystemBasedAdapterFactory implements IGenericDatasetAdapterFactory {
    private static final long serialVersionUID = 1L;
    
    public static final String KEY_FILE_SYSTEM = "fs";
    public static final String LOCAL_FS = "localfs";
    public static final String HDFS = "hdfs";
    public static final String KEY_PATH = "path";
    public static final String KEY_FORMAT = "format";

    private IGenericDatasetAdapterFactory adapterFactory;
    private String format;
    private boolean setup = false;

    @Override
    public IDatasourceAdapter createAdapter(Map<String, Object> configuration, IAType type) throws Exception {
        if (!setup) {
            checkRequiredArgs(configuration);
            String fileSystem = (String) configuration.get(KEY_FILE_SYSTEM);
            String adapterFactoryClass = null;
            if (fileSystem.equalsIgnoreCase(LOCAL_FS)) {
                adapterFactoryClass = "edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory";
            } else if (fileSystem.equals(HDFS)) {
                adapterFactoryClass = "edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory";
            } else {
                throw new AsterixException("Unsupported file system type " + fileSystem);
            }
            format = (String) configuration.get(KEY_FORMAT);
            adapterFactory = (IGenericDatasetAdapterFactory) Class.forName(adapterFactoryClass).newInstance();
            setup = true;
        }
        return new RateControlledFileSystemBasedAdapter((ARecordType) type, configuration,
                (FileSystemBasedAdapter) adapterFactory.createAdapter(configuration, type), format);
    }

    @Override
    public String getName() {
        return "file_feed";
    }

    private void checkRequiredArgs(Map<String, Object> configuration) throws Exception {
        if (configuration.get(KEY_FILE_SYSTEM) == null) {
            throw new Exception("File system type not specified. (fs=?) File system could be 'localfs' or 'hdfs'");
        }
        if (configuration.get(IGenericDatasetAdapterFactory.KEY_TYPE_NAME) == null) {
            throw new Exception("Record type not specified (output-type-name=?)");
        }
        if (configuration.get(KEY_PATH) == null) {
            throw new Exception("File path not specified (path=?)");
        }
        if (configuration.get(KEY_FORMAT) == null) {
            throw new Exception("File format not specified (format=?)");
        }
    }

}