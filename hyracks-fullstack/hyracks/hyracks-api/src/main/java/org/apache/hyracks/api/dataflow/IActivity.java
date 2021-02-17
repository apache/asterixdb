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

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IActivity extends Serializable {
    ActivityId getActivityId();

    IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions) throws HyracksDataException;

    default String getDisplayName() {
        return DisplayNameHelper.toDisplayName(getClass().getName());
    }

    class DisplayNameHelper {
        static final Pattern PREFIX_PATTERN = Pattern.compile("\\B\\w+(\\.[a-z])");

        private DisplayNameHelper() {
        }

        static String toDisplayName(String className) {
            return PREFIX_PATTERN.matcher(className).replaceAll("$1");
        }
    }
}
