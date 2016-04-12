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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class NoMergePolicyFactory implements ILSMMergePolicyFactory {

    private static final long serialVersionUID = 1L;

    private static final String[] SET_VALUES = new String[] {};
    private static final Set<String> PROPERTIES_NAMES = new HashSet<String>(Arrays.asList(SET_VALUES));

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> properties, IHyracksTaskContext ctx) {
        ILSMMergePolicy policy = new NoMergePolicy();
        policy.configure(properties);
        return policy;
    }

    @Override
    public String getName() {
        return "no-merge";
    }

    @Override
    public Set<String> getPropertiesNames() {
        return PROPERTIES_NAMES;
    }

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> properties, IIndexLifecycleManager ilcm) {
        ILSMMergePolicy policy = new NoMergePolicy();
        policy.configure(properties);
        return policy;
    }
}
