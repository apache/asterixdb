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

package org.apache.asterix.om.base;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class AGeneratedUUID extends AUUID {

    private static class Holder {
        static final byte[] hostUnique = new byte[4];

        static {
            new SecureRandom().nextBytes(hostUnique);
        }
    }

    static final Random random = new Random();
    static final AtomicInteger nextInstance = new AtomicInteger(random.nextInt());

    public AGeneratedUUID() {
        int unique = nextInstance.getAndIncrement();

        // fill with pseudo-random bytes
        random.nextBytes(uuidBytes);

        // overwrite the first four bytes with the host unique value
        System.arraycopy(Holder.hostUnique, 0, uuidBytes, 0, 4);

        // overwrite the next four bytes with the thread unique value
        uuidBytes[5] = (byte) (unique >> 24);
        uuidBytes[6] = (byte) (unique >> 16);
        uuidBytes[7] = (byte) (unique >> 8);
        uuidBytes[8] = (byte) unique;
    }

    public void nextUUID() {
        // increment the UUID value
        for (int i = UUID_BYTES - 1; i >= 8; i--) {
            if (++uuidBytes[i] != 0) {
                break;
            }
        }
    }
}
