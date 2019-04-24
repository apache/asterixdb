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

/**
 * This lowercase string token pointable is for the UTF8 string that doesn't have length bytes in the beginning.
 * This pointable exists to represent a string token.
 * The reason is that when we tokenize a string, each token will contain the length as a separate value.
 * Instead, the length of this string is provided as a parameter.
 */
public final class UTF8StringLowercaseTokenPointable extends AbstractPointable implements IHashable, IComparable {

    public static final UTF8StringLowercaseTokenPointableFactory FACTORY =
            new UTF8StringLowercaseTokenPointableFactory();

    public static final class UTF8StringLowercaseTokenPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new UTF8StringLowercaseTokenPointable();
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
        return UTF8StringUtil.lowerCaseCompareTo(this.bytes, this.start, this.length, bytes, start, length);
    }

    @Override
    public int hash() {
        return UTF8StringUtil.lowerCaseHash(bytes, start, length);
    }

    public static int compare(byte[] bytes, int start, int length, byte[] thatBytes, int thatStart, int thatLength) {
        return UTF8StringUtil.lowerCaseCompareTo(bytes, start, length, thatBytes, thatStart, thatLength);
    }
}
