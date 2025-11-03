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
package org.apache.asterix.external.util.azure.datalake;

public class DatalakeConstants {
    private DatalakeConstants() {
        throw new AssertionError("do not instantiate");
    }

    /*
    The behavior of Data Lake (true file system) is to read the files of the specified prefix only, example:
    storage/myData/personal/file1.json
    storage/myData/personal/file2.json
    storage/myData/file3.json
    If the prefix used is "myData", then only the file file3.json is read. However, if the property "recursive"
    is set to "true" when creating the external dataset, then it goes recursively over all the paths, and the result
    is file1.json, file2.json and file3.json.
     */
    public static final String RECURSIVE_FIELD_NAME = "recursive";
}
