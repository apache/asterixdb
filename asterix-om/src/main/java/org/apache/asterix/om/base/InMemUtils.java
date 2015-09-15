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

public class InMemUtils {

    public final static boolean cursorEquals(IACursor c1, IACursor c2) {
        while (c1.next()) {
            if (!(c2.next())) {
                return false;
            }
            IAObject thisO = c1.get();
            IAObject otherO = c2.get();
            if (!(thisO.equals(otherO))) {
                return false;
            }
        }
        if (c2.next()) {
            return false;
        } else {
            return true;
        }
    }

    public final static int hashCursor(IACursor c) {
        int h = 0;
        while (c.next()) {
            h = h * 31 + c.get().hashCode();
        }
        return h;
    }

    static int hashDouble(double value) {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    static boolean deepEqualArrays(IAObject[] v1, IAObject[] v2) {
        if (v1.length != v2.length) {
            return false;
        }
        for (int i = 0; i < v1.length; i++) {
            if (!v1[i].deepEqual(v2[i])) {
                return false;
            }
        }
        return true;
    }
}
