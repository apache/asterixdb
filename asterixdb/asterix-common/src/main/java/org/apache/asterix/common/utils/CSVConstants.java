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
package org.apache.asterix.common.utils;

public class CSVConstants {

    private CSVConstants() {
    }

    public static final String KEY_HEADER = "header";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_RECORD_DELIMITER = "record-delimiter";
    public static final String KEY_QUOTE = "quote";
    public static final String KEY_FORCE_QUOTE = "force-quote";
    public static final String KEY_EMPTY_STRING_AS_NULL = "empty-string-as-null";
    public static final String KEY_ESCAPE = "escape";

    // a string representing the NULL value
    public static final String KEY_NULL_STR = "null";
}
