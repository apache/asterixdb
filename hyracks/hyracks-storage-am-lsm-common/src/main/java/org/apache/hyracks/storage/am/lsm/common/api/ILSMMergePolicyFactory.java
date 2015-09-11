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
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;

public interface ILSMMergePolicyFactory extends Serializable {
    // Having two methods to create the merge policy with two different signatures is hacky, but we do it now
    // because we want to have an access to the IIndexLifecycleManager inside some of the merge policies. However, 
    // in order to get the IIndexLifecycleManager instance, we need to cast to IAsterixAppRuntimeContext which exist
    // in asterix and cannot be seen in hyracks. Thus we pass IHyracksTaskContext and let the merge policy do the casting.
    public ILSMMergePolicy createMergePolicy(Map<String, String> configuration, IHyracksTaskContext ctx);

    public ILSMMergePolicy createMergePolicy(Map<String, String> configuration, IIndexLifecycleManager ilcm);

    public String getName();

    public Set<String> getPropertiesNames();
}