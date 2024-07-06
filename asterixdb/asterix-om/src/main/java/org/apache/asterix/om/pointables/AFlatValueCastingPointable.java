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

package org.apache.asterix.om.pointables;

import org.apache.asterix.om.pointables.base.ICastingPointable;
import org.apache.asterix.om.pointables.visitor.ICastingPointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AFlatValueCastingPointable extends AbstractCastingPointable {

    public static final ICastingPointable nullPointable =
            new AFlatValueCastingPointable(new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG });
    public static final ICastingPointable missingPointable =
            new AFlatValueCastingPointable(new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG });
    public static final IObjectFactory<AFlatValueCastingPointable, IAType> FACTORY =
            type -> new AFlatValueCastingPointable();

    private AFlatValueCastingPointable() {

    }

    private AFlatValueCastingPointable(byte[] bytes) {
        set(bytes, 0, bytes.length);
    }

    @Override
    public <R, T> R accept(ICastingPointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public Type getType() {
        return Type.FLAT;
    }
}
