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
package org.apache.asterix.common.storage;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;

/**
 * An interface for the compression manager which handles all the registered
 * schemes and validates the provided configurations.
 */
public interface ICompressionManager {

    /**
     * Get a registered CompressorDecompressorFactory
     *
     * @param schemeName
     *            Compression scheme name
     * @return Compressor/Decompressor factory if the scheme is specified or NOOP o.w
     * @throws CompilationException
     */
    ICompressorDecompressorFactory getFactory(String schemeName) throws CompilationException;

    /**
     * Get the specified compression scheme in the DDL or the default one
     *
     * @param ddlScheme
     *            Compression scheme name from DDL
     * @return DDL or default compression scheme name
     * @throws CompilationException
     */
    String getDdlOrDefaultCompressionScheme(String ddlScheme) throws CompilationException;
}
