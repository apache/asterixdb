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

package org.apache.hyracks.dataflow.std.buffermanager;

/**
 * Under the big-object setting, there could be multiple variable size free slots to use.
 * In that case, we need to decide which free slot to give back to caller.
 */
public enum EnumFreeSlotPolicy {
    /**
     * Choose the minimum size frame
     */
    SMALLEST_FIT,
    /**
     * Choose the latest used frame if it is big enough
     */
    LAST_FIT,
    /**
     * Choose the largest size frame
     */
    BIGGEST_FIT,
}
