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
package org.apache.asterix.common.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GlobalConfig {

    public static final String ASTERIX_LOGGER_NAME = "org.apache.asterix";
    public static final Logger ASTERIX_LOGGER = LogManager.getLogger(ASTERIX_LOGGER_NAME);
    public static final String CONFIG_FILE_PROPERTY = "AsterixConfigFileName";
    public static final int DEFAULT_FRAME_SIZE = 32768;
    public static final int DEFAULT_INPUT_DATA_COLUMN = 0;
}