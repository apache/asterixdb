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
package edu.uci.ics.asterix.om.types.hierachy;

import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;

public abstract class AbstractIntegerTypePromoteComputer implements ITypePromoteComputer {

    public void promoteIntegerType(byte[] data, int start, int length, IMutableValueStorage storageForPromotedValue,
            ATypeTag targetType, int targetTypeLength) throws IOException {
        storageForPromotedValue.getDataOutput().writeByte(targetType.serialize());
        long num = 0;
        for (int i = start; i < start + length; i++) {
            num += (data[i] & 0xff) << ((length - 1 - (i - start)) * 8);
        }

        for (int i = targetTypeLength - 1; i >= 0; i--) {
            storageForPromotedValue.getDataOutput().writeByte((byte)((num >>> (i * 8)) & 0xFF));
        }
    }
}
