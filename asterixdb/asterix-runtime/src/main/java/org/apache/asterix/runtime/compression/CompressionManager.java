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
package org.apache.asterix.runtime.compression;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressorFactory;

public class CompressionManager implements ICompressionManager {
    private static final Map<String, Class<? extends ICompressorDecompressorFactory>> REGISTERED_SCHEMES =
            getRegisteredSchemes();
    public static final String NONE = "none";
    private final String defaultScheme;

    /*
     * New compression schemes can be added by registering the name and the factory class
     *
     * WARNING: Changing scheme name will breakdown storage back compatibility. Before upgrading to a newer
     * version of the registered schemes, make sure it is also back-compatible with the previous version.
     */
    private static Map<String, Class<? extends ICompressorDecompressorFactory>> getRegisteredSchemes() {
        final Map<String, Class<? extends ICompressorDecompressorFactory>> registeredSchemes = new HashMap<>();
        //No compression
        registeredSchemes.put(NONE, NoOpCompressorDecompressorFactory.class);
        registeredSchemes.put("snappy", SnappyCompressorDecompressorFactory.class);
        return registeredSchemes;
    }

    public CompressionManager(StorageProperties storageProperties) {
        validateCompressionConfiguration(storageProperties);
        defaultScheme = storageProperties.getCompressionScheme();
    }

    @Override
    public ICompressorDecompressorFactory getFactory(String schemeName) throws CompilationException {
        final String scheme = getDdlOrDefaultCompressionScheme(schemeName);
        Class<? extends ICompressorDecompressorFactory> clazz = REGISTERED_SCHEMES.get(scheme);
        try {
            return clazz.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IllegalStateException("Failed to instantiate compressor/decompressor: " + scheme, e);
        }
    }

    @Override
    public String getDdlOrDefaultCompressionScheme(String ddlScheme) throws CompilationException {
        if (ddlScheme != null && !isRegisteredScheme(ddlScheme)) {
            throw new CompilationException(ErrorCode.UNKNOWN_COMPRESSION_SCHEME, ddlScheme, formatSupportedValues());
        }

        return ddlScheme != null ? ddlScheme : defaultScheme;
    }

    /**
     * Register factory classes for persisted resources
     *
     * @param registeredClasses
     */
    public static void registerCompressorDecompressorsFactoryClasses(
            Map<String, Class<? extends IJsonSerializable>> registeredClasses) {
        for (Class<? extends ICompressorDecompressorFactory> clazz : REGISTERED_SCHEMES.values()) {
            registeredClasses.put(clazz.getSimpleName(), clazz);
        }
    }

    /**
     * @param schemeName
     *            Compression scheme name
     * @return
     *         true if it is registered
     */
    public static boolean isRegisteredScheme(String schemeName) {
        return schemeName != null && REGISTERED_SCHEMES.containsKey(schemeName.toLowerCase());
    }

    /**
     * Validate the configuration of StorageProperties
     *
     * @param storageProperties
     */
    private void validateCompressionConfiguration(StorageProperties storageProperties) {
        if (!isRegisteredScheme(storageProperties.getCompressionScheme())) {
            final String option = StorageProperties.Option.STORAGE_COMPRESSION_BLOCK.ini();
            final String value = storageProperties.getCompressionScheme();
            throw new IllegalStateException("Invalid compression configuration (" + option + " = " + value
                    + "). Valid values are: " + formatSupportedValues());
        }

    }

    private String formatSupportedValues() {
        final StringBuilder schemes = new StringBuilder();
        final Iterator<String> iterator = REGISTERED_SCHEMES.keySet().iterator();
        schemes.append('[');
        schemes.append(iterator.next());
        while (iterator.hasNext()) {
            schemes.append(',');
            schemes.append(iterator.next());
        }
        schemes.append(']');
        return schemes.toString();
    }
}
