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

import java.net.Inet4Address;
import java.net.UnknownHostException;

public class SpatialIndexProfiler {
    public static final SpatialIndexProfiler INSTANCE = new SpatialIndexProfiler();
    public static final String PROFILE_HOME_DIR = "/data/seok.kim/spatial-index-experiment/asterix-instance/logs/";
    //    public static final String PROFILE_HOME_DIR = "/Volumes/MyPassport/workspace/spatial-index-experiment/asterix-instance/logs/";
    public ExperimentProfiler falsePositivePerQuery;
    public ExperimentProfiler cacheMissPerQuery;

    private SpatialIndexProfiler() {
        if (ExperimentProfiler.PROFILE_MODE) {
            //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS");
            try {
                falsePositivePerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "falsePositivePerQuery-"
                        + Inet4Address.getLocalHost().getHostAddress() + ".txt", 1);
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            falsePositivePerQuery.begin();
            try {
                cacheMissPerQuery = new ExperimentProfiler(
                        PROFILE_HOME_DIR + "cacheMissPerQuery-" + Inet4Address.getLocalHost().getHostAddress() + ".txt",
                        1);
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            cacheMissPerQuery.begin();
        }
    }
}
