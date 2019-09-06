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

import org.apache.asterix.app.function.ActiveRequestsRewriter;
import org.apache.asterix.app.function.CompletedRequestsRewriter;
import org.apache.asterix.app.function.DatasetResourcesRewriter;
import org.apache.asterix.app.function.DatasetRewriter;
import org.apache.asterix.app.function.DumpIndexRewriter;
import org.apache.asterix.app.function.FeedRewriter;
import org.apache.asterix.app.function.JobSummariesRewriter;
import org.apache.asterix.app.function.PingRewriter;
import org.apache.asterix.app.function.StorageComponentsRewriter;
import org.apache.asterix.app.function.TPCDSAllTablesDataGeneratorRewriter;
import org.apache.asterix.app.function.TPCDSSingleTableDataGeneratorRewriter;
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
                DatasetResourcesRewriter.INSTANCE, BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
        // Dataset components function
        BuiltinFunctions.addPrivateFunction(StorageComponentsRewriter.STORAGE_COMPONENTS,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(StorageComponentsRewriter.STORAGE_COMPONENTS, false);
        BuiltinFunctions.addDatasourceFunction(StorageComponentsRewriter.STORAGE_COMPONENTS,
                StorageComponentsRewriter.INSTANCE, BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
        // Ping function
        BuiltinFunctions.addPrivateFunction(PingRewriter.PING,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(PingRewriter.PING, true);
        BuiltinFunctions.addDatasourceFunction(PingRewriter.PING, PingRewriter.INSTANCE,
                BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
        // TPC-DS data generation function
        BuiltinFunctions.addPrivateFunction(TPCDSSingleTableDataGeneratorRewriter.TPCDS_SINGLE_TABLE_DATA_GENERATOR,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(TPCDSSingleTableDataGeneratorRewriter.TPCDS_SINGLE_TABLE_DATA_GENERATOR, true);
        BuiltinFunctions.addDatasourceFunction(TPCDSSingleTableDataGeneratorRewriter.TPCDS_SINGLE_TABLE_DATA_GENERATOR,
                TPCDSSingleTableDataGeneratorRewriter.INSTANCE);
        BuiltinFunctions.addPrivateFunction(TPCDSAllTablesDataGeneratorRewriter.TPCDS_ALL_TABLES_DATA_GENERATOR,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(TPCDSAllTablesDataGeneratorRewriter.TPCDS_ALL_TABLES_DATA_GENERATOR, true);
        BuiltinFunctions.addDatasourceFunction(TPCDSAllTablesDataGeneratorRewriter.TPCDS_ALL_TABLES_DATA_GENERATOR,
                TPCDSAllTablesDataGeneratorRewriter.INSTANCE);
        // Active requests function
        BuiltinFunctions.addFunction(ActiveRequestsRewriter.ACTIVE_REQUESTS,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(ActiveRequestsRewriter.ACTIVE_REQUESTS, true);
        BuiltinFunctions.addDatasourceFunction(ActiveRequestsRewriter.ACTIVE_REQUESTS, ActiveRequestsRewriter.INSTANCE,
                BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
        // job-summaries function
        BuiltinFunctions.addPrivateFunction(JobSummariesRewriter.JOBSUMMARIES,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(JobSummariesRewriter.JOBSUMMARIES, true);
        BuiltinFunctions.addDatasourceFunction(JobSummariesRewriter.JOBSUMMARIES, JobSummariesRewriter.INSTANCE,
                BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
        // completed requests function
        BuiltinFunctions.addFunction(CompletedRequestsRewriter.COMPLETED_REQUESTS,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(CompletedRequestsRewriter.COMPLETED_REQUESTS, true);
        BuiltinFunctions.addDatasourceFunction(CompletedRequestsRewriter.COMPLETED_REQUESTS,
                CompletedRequestsRewriter.INSTANCE, BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
        // Dump index function
        BuiltinFunctions.addPrivateFunction(DumpIndexRewriter.DUMP_INDEX,
                (expression, env, mp) -> RecordUtil.FULLY_OPEN_RECORD_TYPE, true);
        BuiltinFunctions.addUnnestFun(DumpIndexRewriter.DUMP_INDEX, false);
        BuiltinFunctions.addDatasourceFunction(DumpIndexRewriter.DUMP_INDEX, DumpIndexRewriter.INSTANCE);
    }

    private MetadataBuiltinFunctions() {
    }

    public static void init() {
        // Only execute the static block
    }
}
