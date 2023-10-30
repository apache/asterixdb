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
package org.apache.asterix.runtime.writer;

import java.io.OutputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * An {@link IExternalFileWriter} printer
 */
public interface IExternalFilePrinter {

    /**
     * Open the printer
     */
    void open() throws HyracksDataException;

    /**
     * Initialize the printer with a new stream
     *
     * @param outputStream to print to
     */
    void newStream(OutputStream outputStream) throws HyracksDataException;

    /**
     * Print the provided value
     *
     * @param value to print
     */
    void print(IValueReference value) throws HyracksDataException;

    /**
     * Flush and close the printer
     */
    void close() throws HyracksDataException;
}
