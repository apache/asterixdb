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
package org.apache.asterix.util;

import org.apache.asterix.app.function.DatasetResourcesRewriter;
import org.apache.asterix.app.function.DatasetRewriter;
import org.apache.asterix.app.function.FeedRewriter;
import org.apache.asterix.app.function.PingRewriter;
import org.apache.asterix.app.function.StorageComponentsRewriter;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.RecordUtil;

public class MetadataBuiltinFunctions {

    static {
        // Dataset function
        BuiltinFunctions.addFunction(BuiltinFunctions.DATASET, DatasetRewriter.INSTANCE, true);
        BuiltinFunctions.addUnnestFun(BuiltinFunctions.DATASET, false);
        BuiltinFunctions.addDatasourceFunction(BuiltinFunctions.DATASET, DatasetRewriter.INSTANCE);
        // Feed collect function
        BuiltinFunctions.addPrivateFunction(BuiltinFunctions.FEED_COLLECT, FeedRewriter.INSTANCE, true);
        BuiltinFunctions.addUnnestFun(BuiltinFunctions.FEED_COLLECT, false);
        BuiltinFunctions.addDatasourceFunction(BuiltinFunctions.FEED_COLLECT, FeedRewriter.INSTANCE);
        // Dataset resources function
        BuiltinFunctions.addPrivateFunction(DatasetResourcesRewriter.DATASET_RESOURCES,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(DatasetResourcesRewriter.DATASET_RESOURCES, false);
        BuiltinFunctions.addDatasourceFunction(DatasetResourcesRewriter.DATASET_RESOURCES,
                DatasetResourcesRewriter.INSTANCE);
        // Dataset components function
        BuiltinFunctions.addPrivateFunction(StorageComponentsRewriter.STORAGE_COMPONENTS,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(StorageComponentsRewriter.STORAGE_COMPONENTS, false);
        BuiltinFunctions.addDatasourceFunction(StorageComponentsRewriter.STORAGE_COMPONENTS,
                StorageComponentsRewriter.INSTANCE);
        // Ping function
        BuiltinFunctions.addPrivateFunction(PingRewriter.PING,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(PingRewriter.PING, true);
        BuiltinFunctions.addDatasourceFunction(PingRewriter.PING, PingRewriter.INSTANCE);
    }

    private MetadataBuiltinFunctions() {
    }

    public static void init() {
        // Only execute the static block
    }
}
