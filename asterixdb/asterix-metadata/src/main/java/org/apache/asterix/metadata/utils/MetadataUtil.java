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
package org.apache.asterix.metadata.utils;

public class MetadataUtil {
    public static final int PENDING_NO_OP = 0;
    public static final int PENDING_ADD_OP = 1;
    public static final int PENDING_DROP_OP = 2;

    private MetadataUtil() {
    }

    public static String pendingOpToString(int pendingOp) {
        switch (pendingOp) {
            case PENDING_NO_OP:
                return "Pending No Operation";
            case PENDING_ADD_OP:
                return "Pending Add Operation";
            case PENDING_DROP_OP:
                return "Pending Drop Operation";
            default:
                return "Unknown Pending Operation";
        }
    }

    public static String getDataverseFromFullyQualifiedName(String datasetName) {
        int idx = datasetName.indexOf('.');
        return datasetName.substring(0, idx);
    }
}
