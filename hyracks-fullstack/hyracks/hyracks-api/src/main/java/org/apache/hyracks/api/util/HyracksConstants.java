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
package org.apache.hyracks.api.util;

public class HyracksConstants {
    public static final String KEY_MESSAGE = "HYX:MSG";
    public static final String HYRACKS_LOGGER_NAME = "org.apache.hyracks";
    // A frame manager that manages all inverted index searches
    public static final String INVERTED_INDEX_SEARCH_FRAME_MANAGER = "INVERTED_INDEX_SEARCH_FRAME_MANAGER";
    // Hyracks task context
    public static final String HYRACKS_TASK_CONTEXT = "HYRACKS_TASK_CONTEXT";

    public static final String INDEX_CURSOR_STATS = "INDEX_CURSOR_STATS";

    private HyracksConstants() {
    }
}
