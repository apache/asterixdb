/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.transaction;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TreeLoggerRepository {

    private static final Map<ByteBuffer, TreeLogger> loggers = new HashMap<ByteBuffer, TreeLogger>();

    public static synchronized TreeLogger getTreeLogger(byte[] resourceIdBytes) {
        ByteBuffer resourceId = ByteBuffer.wrap(resourceIdBytes);
        TreeLogger logger = loggers.get(resourceId);
        if (logger == null) {
            logger = new TreeLogger(resourceIdBytes);
            loggers.put(resourceId, logger);
        }
        return logger;
    }
}