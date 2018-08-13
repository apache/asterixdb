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
package org.apache.asterix.external.classad;

import java.nio.charset.StandardCharsets;

public class Common {
    public static final String ATTR_AD = "Ad";
    public static final String ATTR_CONTEXT = "Context";
    public static final String ATTR_DEEP_MODS = "DeepMods";
    public static final String ATTR_DELETE_AD = "DeleteAd";
    public static final String ATTR_DELETES = "Deletes";
    public static final String ATTR_KEY = "Key";
    public static final String ATTR_NEW_AD = "NewAd";
    public static final String ATTR_OP_TYPE = "OpType";
    public static final String ATTR_PARENT_VIEW_NAME = "ParentViewName";
    public static final String ATTR_PARTITION_EXPRS = "PartitionExprs";
    public static final String ATTR_PARTITIONED_VIEWS = "PartitionedViews";
    public static final String ATTR_PROJECT_THROUGH = "ProjectThrough";
    public static final String ATTR_RANK_HINTS = "RankHints";
    public static final String ATTR_REPLACE = "Replace";
    public static final String ATTR_SUBORDINATE_VIEWS = "SubordinateViews";
    public static final String ATTR_UPDATES = "Updates";
    public static final String ATTR_WANT_LIST = "WantList";
    public static final String ATTR_WANT_PRELUDE = "WantPrelude";
    public static final String ATTR_WANT_RESULTS = "WantResults";
    public static final String ATTR_WANT_POSTLUDE = "WantPostlude";
    public static final String ATTR_VIEW_INFO = "ViewInfo";
    public static final String ATTR_VIEW_NAME = "ViewName";
    public static final String ATTR_XACTION_NAME = "XactionName";
    public static final String ATTR_REQUIREMENTS = "Requirements";
    public static final String ATTR_RANK = "Rank";

    public static class CaseIgnLTStr {
        public static boolean call(String s1, String s2) {
            return (s1.compareToIgnoreCase(s2) < 0);
        }
    }

    public static class ClassadAttrNameHash {
        public static int call(String s) {
            int h = 0;
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            for (byte ch : bytes) {
                h = 5 * h + (ch | 0x20);
            }
            return h;
        }
    }
}
