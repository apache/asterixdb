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
package org.apache.hyracks.storage.am.common.api;

import java.io.Closeable;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.LocalResource;

public interface IIndexDataflowHelper extends Closeable {

    /**
     * If open throws an exception, it means that the index was not opened successfully.
     *
     * @throws HyracksDataException
     */
    void open() throws HyracksDataException;

    /**
     * If close throws an exception, it means that the index was not closed successfully.
     *
     * @throws HyracksDataException
     */
    @Override
    void close() throws HyracksDataException;

    /**
     * Removes the index permanently.
     *
     * @throws HyracksDataException
     */
    void destroy() throws HyracksDataException;

    /**
     * @return the index instance
     */
    IIndex getIndexInstance();

    /**
     * @return the serializable resource representing the index
     * @throws HyracksDataException
     */
    LocalResource getResource() throws HyracksDataException;
}
