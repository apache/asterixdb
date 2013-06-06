/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.config;

public class AsterixCompilerProperties extends AbstractAsterixProperties {
    private static final String COMPILER_SORTMEMORY_KEY = "compiler.sortmemory";
    private static final long COMPILER_SORTMEMORY_DEFAULT = (512 << 20); // 512MB

    private static final String COMPILER_JOINMEMORY_KEY = "compiler.joinmemory";
    private static final long COMPILER_JOINMEMORY_DEFAULT = (512 << 20); // 512MB

    private static final String COMPILER_FRAMESIZE_KEY = "compiler.framesize";
    private static int COMPILER_FRAMESIZE_DEFAULT = (32 << 10); // 32KB

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

    public int getFrameSize() {
        return accessor.getProperty(COMPILER_FRAMESIZE_KEY, COMPILER_FRAMESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }
}
