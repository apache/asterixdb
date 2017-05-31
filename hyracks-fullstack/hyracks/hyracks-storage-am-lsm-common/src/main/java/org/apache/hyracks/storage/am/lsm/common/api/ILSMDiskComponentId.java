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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;

/**
 * Stores the id of the disk component, which is a interval (minId, maxId).
 * When a disk component is formed by the flush operation, its initial minId and maxId are the same, and
 * currently are set as the flush LSN.
 * When a disk component is formed by the merge operation, its [minId, maxId] is set as the union of
 * all ids of merged disk components.
 *
 * @author luochen
 *
 */
public interface ILSMDiskComponentId {

    public static final long NOT_FOUND = -1;

    public static final MutableArrayValueReference COMPONENT_ID_MIN_KEY =
            new MutableArrayValueReference("Component_Id_Min".getBytes());

    public static final MutableArrayValueReference COMPONENT_ID_MAX_KEY =
            new MutableArrayValueReference("Component_Id_Max".getBytes());

    long getMinId();

    long getMaxId();

    default boolean notFound() {
        return getMinId() == NOT_FOUND || getMaxId() == NOT_FOUND;
    }

}
