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
package org.apache.asterix.external.api;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A super interface implemented by a data source adapter. An adapter can be a
 * pull based or push based. This interface provides all common APIs that need
 * to be implemented by each adapter irrespective of the the kind of
 * adapter(pull or push).
 */
@FunctionalInterface
public interface IDataSourceAdapter {

    public enum AdapterType {
        INTERNAL,
        EXTERNAL
    }

    /**
     * Triggers the adapter to begin ingesting data from the external source.
     *
     * @param partition
     *            The adapter could be running with a degree of parallelism.
     *            partition corresponds to the i'th parallel instance.
     * @param writer
     *            The instance of frame writer that is used by the adapter to
     *            write frame to. Adapter packs the fetched bytes (from external source),
     *            packs them into frames and forwards the frames to an upstream receiving
     *            operator using the instance of IFrameWriter.
     * @throws Exception
     */
    public void start(int partition, IFrameWriter writer) throws HyracksDataException, InterruptedException;
}
