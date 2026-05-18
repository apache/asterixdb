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
package org.apache.hyracks.storage.am.vector.utils;

import java.nio.charset.StandardCharsets;

import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;

/**
 * Keys for the VTree component metadata frame. Written by the static-structure builder,
 * the bulk loader, and the flush loader; read back by the bulk loader and {@link
 * org.apache.hyracks.storage.am.vector.impls.VTree} when a component is opened. The
 * writer and reader sides must agree, so the keys live here as a single source of truth.
 * <p>
 * Exposed as reusable {@link MutableArrayValueReference} constants: the metadata frame's put/get COPY the
 * key bytes, so a single shared, read-only reference is safe and avoids re-encoding the key on every access.
 * The stored key bytes are short codes to keep the on-disk metadata compact.
 */
public final class VTreeMetadataKeys {

    private VTreeMetadataKeys() {
    }

    /** Number of leaf centroids in the component (stored key {@code "VTNLC"}). */
    public static final MutableArrayValueReference NUM_LEAF_CENTROIDS =
            new MutableArrayValueReference("VTNLC".getBytes(StandardCharsets.UTF_8));

    /** Centroid id of the first leaf centroid, BFS-from-root numbering (stored key {@code "VTFLC"}). */
    public static final MutableArrayValueReference FIRST_LEAF_CENTROID_ID =
            new MutableArrayValueReference("VTFLC".getBytes(StandardCharsets.UTF_8));
}
