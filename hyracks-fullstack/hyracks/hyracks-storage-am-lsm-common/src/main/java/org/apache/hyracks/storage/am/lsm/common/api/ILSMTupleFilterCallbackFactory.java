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

import java.io.Serializable;

public interface ILSMTupleFilterCallbackFactory extends Serializable {
    /**
     * Creates a callback function that utilizes the field permutation of the incoming tuple.
     * The field tuple contains information about the position of the record, meta, and primary key,
     * facilitating the extraction of relevant information for filtering purposes.
     */
    ILSMTupleFilterCallback createTupleFilterCallback(int[] fieldPermutation);
}
