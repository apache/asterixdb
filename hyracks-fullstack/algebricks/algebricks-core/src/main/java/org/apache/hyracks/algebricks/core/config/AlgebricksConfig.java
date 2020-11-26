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
package org.apache.hyracks.algebricks.core.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AlgebricksConfig {

    public static final String ALGEBRICKS_LOGGER_NAME = "org.apache.hyracks.algebricks";
    public static final Logger ALGEBRICKS_LOGGER = LogManager.getLogger(ALGEBRICKS_LOGGER_NAME);
    public static final int SORT_SAMPLES_DEFAULT = 100;
    public static final boolean SORT_PARALLEL_DEFAULT = true;
    public static final boolean INDEX_ONLY_DEFAULT = true;
    public static final boolean SANITYCHECK_DEFAULT = false;
    public static final boolean EXTERNAL_FIELD_PUSHDOWN_DEFAULT = false;
}
