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
package org.apache.asterix.external.library;

import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static org.apache.asterix.common.library.LibraryDescriptor.DESCRIPTOR_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibrary;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ExternalLibraryManager implements ILibraryManager {

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.configure(SORT_PROPERTIES_ALPHABETICALLY, true);
        OBJECT_MAPPER.configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<Pair<DataverseName, String>, ILibrary> libraries = Collections.synchronizedMap(new HashMap());
    private final IPersistedResourceRegistry reg;

    public ExternalLibraryManager(File appDir, IPersistedResourceRegistry reg) {
        this.reg = reg;
        scanLibraries(appDir);
    }

    @Override
    public void scanLibraries(File appDir) {
        File[] libs = appDir.listFiles((dir, name) -> dir.isDirectory());
        if (libs != null) {
            for (File lib : libs) {
                String libraryPath = lib.getAbsolutePath();
                try {
                    setUpDeployedLibrary(libraryPath);
                } catch (AsterixException | IOException e) {
                    LOGGER.error("Unable to properly initialize external libraries", e);
                }
            }
        }
    }

    public void register(DataverseName dataverseName, String libraryName, ILibrary library) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        libraries.put(key, library);
    }

    @Override
    public void deregister(DataverseName dataverseName, String libraryName) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        ILibrary cl = libraries.get(key);
        if (cl != null) {
            cl.close();
            libraries.remove(key);
        }
    }

    public void setUpDeployedLibrary(String libraryPath) throws IOException, AsterixException {
        // get the installed library dirs
        String[] parts = libraryPath.split(File.separator);
        DataverseName catenatedDv = DataverseName.createFromCanonicalForm(parts[parts.length - 1]);
        String name = catenatedDv.getParts().get(catenatedDv.getParts().size() - 1);
        DataverseName dataverse = DataverseName.create(catenatedDv.getParts(), 0, catenatedDv.getParts().size() - 1);
        try {
            File langFile = new File(libraryPath, DESCRIPTOR_NAME);
            final JsonNode jsonNode = OBJECT_MAPPER.readValue(Files.readAllBytes(langFile.toPath()), JsonNode.class);
            LibraryDescriptor desc = (LibraryDescriptor) reg.deserialize(jsonNode);
            ExternalFunctionLanguage libLang = desc.getLang();
            switch (libLang) {
                case JAVA:
                    register(dataverse, name, new JavaLibrary(libraryPath));
                    break;
                case PYTHON:
                    register(dataverse, name, new PythonLibrary(libraryPath));
                    break;
                default:
                    throw new IllegalStateException("Library path file refers to unknown language");
            }
        } catch (IOException | AsterixException e) {
            LOGGER.error("Failed to initialized library", e);
            throw e;
        }
    }

    @Override
    public ILibrary getLibrary(DataverseName dataverseName, String libraryName) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        return libraries.get(key);
    }

    private static Pair<DataverseName, String> getKey(DataverseName dataverseName, String libraryName) {
        return new Pair<>(dataverseName, libraryName);
    }

}
