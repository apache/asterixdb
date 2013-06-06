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
package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;

@SuppressWarnings("unchecked")
public final class RecordDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ISerializerDeserializer[] fields;
    private final ITypeTraits[] typeTraits;

    // leaving this constructor for backwards-compatibility
    public RecordDescriptor(ISerializerDeserializer[] fields) {
        this.fields = fields;
        this.typeTraits = null;
    }

    // temporarily adding constructor to include type traits
    public RecordDescriptor(ISerializerDeserializer[] fields, ITypeTraits[] typeTraits) {
        this.fields = fields;
        this.typeTraits = typeTraits;
    }

    public int getFieldCount() {
        return fields.length;
    }

    public ISerializerDeserializer[] getFields() {
        return fields;
    }

    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
    }
}