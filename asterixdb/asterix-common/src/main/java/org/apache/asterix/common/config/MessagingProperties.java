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
import org.apache.hyracks.util.StorageUtil.StorageUnit;

public class MessagingProperties extends AbstractAsterixProperties {

    private static final String MESSAGING_FRAME_SIZE_KEY = "messaging.frame.size";
    private static final int MESSAGING_FRAME_SIZE_DEFAULT = StorageUtil.getSizeInBytes(4, StorageUnit.KILOBYTE);

    private static final String MESSAGING_FRAME_COUNT_KEY = "messaging.frame.count";
    private static final int MESSAGING_BUFFER_COUNTE_DEFAULT = 512;

    public MessagingProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getFrameSize() {
        return accessor.getProperty(MESSAGING_FRAME_SIZE_KEY, MESSAGING_FRAME_SIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getFrameCount() {
        return accessor.getProperty(MESSAGING_FRAME_COUNT_KEY, MESSAGING_BUFFER_COUNTE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }
}