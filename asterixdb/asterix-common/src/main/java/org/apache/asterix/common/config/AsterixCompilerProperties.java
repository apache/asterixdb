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

import org.apache.hyracks.util.StorageUtil;

import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

public class AsterixCompilerProperties extends AbstractAsterixProperties {
    private static final int MB = 1048576;

    private static final String COMPILER_SORTMEMORY_KEY = "compiler.sortmemory";
    private static final long COMPILER_SORTMEMORY_DEFAULT = StorageUtil.getSizeInBytes(32, MEGABYTE);

    private static final String COMPILER_GROUPMEMORY_KEY = "compiler.groupmemory";
    private static final long COMPILER_GROUPMEMORY_DEFAULT = StorageUtil.getSizeInBytes(32, MEGABYTE);

    private static final String COMPILER_JOINMEMORY_KEY = "compiler.joinmemory";
    private static final long COMPILER_JOINMEMORY_DEFAULT = StorageUtil.getSizeInBytes(32, MEGABYTE);

    private static final String COMPILER_INTERVAL_MAXDURATION_KEY = "compiler.interval.maxduration";
    private static final long COMPILER_INTERVAL_MAXDURATION_DEFAULT = 1000;

    private static final String COMPILER_FRAMESIZE_KEY = "compiler.framesize";
    private static final int COMPILER_FRAMESIZE_DEFAULT = StorageUtil.getSizeInBytes(32, KILOBYTE);

    private static final String COMPILER_JOIN_LEFTINPUT_KEY = "compiler.join.leftinput";
    private static final long COMPILER_JOIN_LEFTINPUT_DEFAULT = (int) ((140L * 1024 * MB) / COMPILER_FRAMESIZE_DEFAULT); // 140GB

    private static final String COMPILER_PREGELIX_HOME = "compiler.pregelix.home";
    private static final String COMPILER_PREGELIX_HOME_DEFAULT = "~/pregelix";

    public AsterixCompilerProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    @PropertyKey(COMPILER_SORTMEMORY_KEY)
    public long getSortMemorySize() {
        return accessor.getProperty(COMPILER_SORTMEMORY_KEY, COMPILER_SORTMEMORY_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    @PropertyKey(COMPILER_JOINMEMORY_KEY)
    public long getJoinMemorySize() {
        return accessor.getProperty(COMPILER_JOINMEMORY_KEY, COMPILER_JOINMEMORY_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public long getJoinLeftInputSize() {
        return accessor.getProperty(COMPILER_JOIN_LEFTINPUT_KEY, COMPILER_JOIN_LEFTINPUT_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public long getGroupMemorySize() {
        return accessor.getProperty(COMPILER_GROUPMEMORY_KEY, COMPILER_GROUPMEMORY_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public long getIntervalMaxDuration() {
        return accessor.getProperty(COMPILER_INTERVAL_MAXDURATION_KEY, COMPILER_INTERVAL_MAXDURATION_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    @PropertyKey(COMPILER_FRAMESIZE_KEY)
    public int getFrameSize() {
        return accessor.getProperty(COMPILER_FRAMESIZE_KEY, COMPILER_FRAMESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    @PropertyKey(COMPILER_PREGELIX_HOME)
    public String getPregelixHome() {
        return accessor.getProperty(COMPILER_PREGELIX_HOME, COMPILER_PREGELIX_HOME_DEFAULT,
                PropertyInterpreters.getStringPropertyInterpreter());
    }
}
