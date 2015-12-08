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

public class AsterixCompilerProperties extends AbstractAsterixProperties {
    private static final String COMPILER_SORTMEMORY_KEY = "compiler.sortmemory";
    private static final long COMPILER_SORTMEMORY_DEFAULT = (32 << 20); // 32MB

    private static final String COMPILER_GROUPMEMORY_KEY = "compiler.groupmemory";
    private static final long COMPILER_GROUPMEMORY_DEFAULT = (32 << 20); // 32MB

    private static final String COMPILER_JOINMEMORY_KEY = "compiler.joinmemory";
    private static final long COMPILER_JOINMEMORY_DEFAULT = (32 << 20); // 32MB

    private static final String COMPILER_FRAMESIZE_KEY = "compiler.framesize";
    private static int COMPILER_FRAMESIZE_DEFAULT = (32 << 10); // 32KB

    private static final String COMPILER_PREGELIX_HOME = "compiler.pregelix.home";
    private static final String COMPILER_PREGELIX_HOME_DEFAULT = "~/pregelix";

    public AsterixCompilerProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public long getSortMemorySize() {
        return accessor.getProperty(COMPILER_SORTMEMORY_KEY, COMPILER_SORTMEMORY_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public long getJoinMemorySize() {
        return accessor.getProperty(COMPILER_JOINMEMORY_KEY, COMPILER_JOINMEMORY_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public long getGroupMemorySize() {
        return accessor.getProperty(COMPILER_GROUPMEMORY_KEY, COMPILER_GROUPMEMORY_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public int getFrameSize() {
        return accessor.getProperty(COMPILER_FRAMESIZE_KEY, COMPILER_FRAMESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public String getPregelixHome() {
        return accessor.getProperty(COMPILER_PREGELIX_HOME, COMPILER_PREGELIX_HOME_DEFAULT,
                PropertyInterpreters.getStringPropertyInterpreter());
    }
}
