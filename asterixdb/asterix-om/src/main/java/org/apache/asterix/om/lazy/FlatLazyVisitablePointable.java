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
package org.apache.asterix.om.lazy;

import java.util.Objects;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FlatLazyVisitablePointable extends AbstractLazyVisitablePointable {
    private final ATypeTag typeTag;

    public FlatLazyVisitablePointable(boolean tagged, ATypeTag typeTag) {
        super(tagged);
        Objects.requireNonNull(typeTag);
        this.typeTag = typeTag;
    }

    @Override
    public byte getSerializedTypeTag() {
        if (isTagged()) {
            return getByteArray()[getStartOffset()];
        }
        return typeTag.serialize();
    }

    @Override
    public ATypeTag getTypeTag() {
        if (isTagged()) {
            return ATypeTag.VALUE_TYPE_MAPPING[getSerializedTypeTag()];
        }
        return typeTag;
    }

    @Override
    public <R, T> R accept(ILazyVisitablePointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    void init(byte[] data, int offset, int length) {
        //noOp
    }
}
