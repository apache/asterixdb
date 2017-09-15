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
package org.apache.hyracks.storage.am.common.util;

public class BitOperationUtils {
    private BitOperationUtils() {
    }

    public static void setBit(byte[] targetBuf, int targetOff, byte bitPosition) {
        targetBuf[targetOff] = (byte) (targetBuf[targetOff] | (1 << bitPosition));
    }

    public static boolean getBit(byte[] targetBuf, int targetOff, byte bitPosition) {
        return (byte) (targetBuf[targetOff] & (1 << bitPosition)) != 0;
    }

    public static int getFlagBytes(int numFlags) {
        return (int) Math.ceil((double) numFlags / 8.0);
    }
}
