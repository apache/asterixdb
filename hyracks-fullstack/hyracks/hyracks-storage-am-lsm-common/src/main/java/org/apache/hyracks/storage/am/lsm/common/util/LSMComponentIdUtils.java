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
package org.apache.hyracks.storage.am.lsm.common.util;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;

public class LSMComponentIdUtils {

    private static final MutableArrayValueReference COMPONENT_ID_MIN_KEY =
            new MutableArrayValueReference("Component_Id_Min".getBytes());

    private static final MutableArrayValueReference COMPONENT_ID_MAX_KEY =
            new MutableArrayValueReference("Component_Id_Max".getBytes());

    private LSMComponentIdUtils() {

    }

    public static ILSMComponentId readFrom(IComponentMetadata metadata) throws HyracksDataException {
        long minId = ComponentUtils.getLong(metadata, COMPONENT_ID_MIN_KEY, LSMComponentId.NOT_FOUND);
        long maxId = ComponentUtils.getLong(metadata, COMPONENT_ID_MAX_KEY, LSMComponentId.NOT_FOUND);
        if (minId == LSMComponentId.NOT_FOUND || maxId == LSMComponentId.NOT_FOUND) {
            return LSMComponentId.MISSING_COMPONENT_ID;
        } else {
            return new LSMComponentId(minId, maxId);
        }
    }

    public static void persist(ILSMComponentId id, IComponentMetadata metadata) throws HyracksDataException {
        LSMComponentId componentId = (LSMComponentId) id;
        metadata.put(COMPONENT_ID_MIN_KEY, LongPointable.FACTORY.createPointable(componentId.getMinId()));
        metadata.put(COMPONENT_ID_MAX_KEY, LongPointable.FACTORY.createPointable(componentId.getMaxId()));
    }

    public static ILSMComponentId union(ILSMComponentId id1, ILSMComponentId id2) {
        long minId = Long.min(((LSMComponentId) id1).getMinId(), ((LSMComponentId) id2).getMinId());
        long maxId = Long.max(((LSMComponentId) id1).getMaxId(), ((LSMComponentId) id2).getMaxId());
        return new LSMComponentId(minId, maxId);
    }

}
