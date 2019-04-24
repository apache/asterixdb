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
package org.apache.hyracks.data.std.primitive;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IHashable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.fasterxml.jackson.databind.JsonNode;

public final class UTF8StringLowercasePointable extends AbstractPointable implements IHashable, IComparable {

    public static final UTF8StringLowercasePointableFactory FACTORY = new UTF8StringLowercasePointableFactory();

    public static final class UTF8StringLowercasePointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new UTF8StringLowercasePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return VarLengthTypeTrait.INSTANCE;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
            return registry.getClassIdentifier(getClass(), serialVersionUID);
        }

        @SuppressWarnings("squid:S1172") // unused parameter
        public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
            return FACTORY;
        }
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        return UTF8StringUtil.lowerCaseCompareTo(this.bytes, this.start, bytes, start);
    }

    @Override
    public int hash() {
        return UTF8StringUtil.lowerCaseHash(bytes, start);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static int compare(byte[] bytes, int start, int length, byte[] thatBytes, int thatStart, int thatLength) {
        return UTF8StringUtil.lowerCaseCompareTo(bytes, start, thatBytes, thatStart);
    }
}
