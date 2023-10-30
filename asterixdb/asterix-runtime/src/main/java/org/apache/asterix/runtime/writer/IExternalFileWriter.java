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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * A file writer
 */
public interface IExternalFileWriter {

    /**
     * Open the writer
     */
    void open() throws HyracksDataException;

    /**
     * Validate the writing directory
     *
     * @param directory to write
     */
    void validate(String directory) throws HyracksDataException;

    /**
     * Initialize the writer to write to a new path
     *
     * @param directory of where to write the file
     * @param fileName  of the file name to create
     * @return true if a new file can be created, false otherwise
     */
    boolean newFile(String directory, String fileName) throws HyracksDataException;

    /**
     * Writer the provided value
     *
     * @param value to write
     */
    void write(IValueReference value) throws HyracksDataException;

    /**
     * Run the abort sequence in case of a failure
     */
    void abort() throws HyracksDataException;

    /**
     * Flush the final result and close the writer
     */
    void close() throws HyracksDataException;
}
