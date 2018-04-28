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
package org.apache.asterix.metadata.api;

import java.util.List;

import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IActiveEntityController extends IActiveEntityEventsListener {

    /**
     * Start the active entity
     *
     * @param metadataProvider
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    void start(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException;

    /**
     * Stop the active entity
     *
     * @param metadataProvider
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    void stop(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException;

    /**
     * Suspend the active entity
     * This call stops and freezes the active entity. The calling thread must call resume to release
     * locks on the entity
     *
     * @param metadataProvider
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    void suspend(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException;

    /**
     * Resumes the active entity activity prior to the suspend call
     *
     * @param metadataProvider
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    void resume(MetadataProvider metadataProvider) throws HyracksDataException, InterruptedException;

    /**
     * Start failure recovery
     *
     */
    void recover();

    /**
     * Add dataset to the list of associated datasets
     *
     * @param dataset
     *            the dataset to add
     * @throws HyracksDataException
     *             if the entity is active
     */
    void add(Dataset dataset) throws HyracksDataException;

    /**
     * Remove dataset from the list of associated datasets
     *
     * @param dataset
     *            the dataset to add
     * @throws HyracksDataException
     *             if the entity is active
     */
    void remove(Dataset dataset) throws HyracksDataException;

    /**
     * @return the list of associated datasets
     */
    List<Dataset> getDatasets();

    /**
     * replace the dataset object with the passed updated object
     *
     * @param target
     */
    void replace(Dataset dataset);

}
