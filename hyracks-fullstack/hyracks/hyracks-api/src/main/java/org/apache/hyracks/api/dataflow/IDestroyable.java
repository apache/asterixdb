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

package org.apache.hyracks.api.dataflow;

import org.apache.hyracks.api.exceptions.HyracksDataException;

@FunctionalInterface
public interface IDestroyable {
    /**
     * Destroy the object and releases any system resources associated
     * with it. If the object is already destroyed then invoking this
     * method has no effect.
     * The behavior of other calls after this method is invoked is undefined
     *
     * @throws HyracksDataException
     */
    void destroy() throws HyracksDataException;
}
