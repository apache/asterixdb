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
package org.apache.hyracks.util.trace;

public class TraceUtils {

    private TraceUtils() {
    }

    // CATEGORIES
    public static final String TRACER = "Tracer";
    public static final String TIMESTAMP = "Timestamp";
    public static final String INDEX_OPERATIONS = "IndexOperations";
    public static final String INDEX_IO_OPERATIONS = "IndexIoOperations";
    public static final String INGESTION = "Ingestion";
    public static final String STORAGE = "Storage";
    public static final String LATENCY = "Latency";

}
