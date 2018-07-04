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

import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * This class represents a flat field, e.g., int field, string field, and
 * so on, based on a binary representation.
 */
public class AFlatValuePointable extends AbstractVisitablePointable {

    /**
     * DO NOT allow to create AFlatValuePointable object arbitrarily, force to
     * use object pool based allocator. The factory is not public so that it
     * cannot called in other places than PointableAllocator.
     */
    static IObjectFactory<AFlatValuePointable, IAType> FACTORY = type -> new AFlatValuePointable();

    /**
     * private constructor, to prevent arbitrary creation
     */
    private AFlatValuePointable() {

    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IValueReference))
            return false;

        // get right raw data
        IValueReference ivf = (IValueReference) o;
        byte[] odata = ivf.getByteArray();
        int ostart = ivf.getStartOffset();
        int olen = ivf.getLength();

        // get left raw data
        byte[] data = getByteArray();
        int start = getStartOffset();
        int len = getLength();

        // bytes length should be equal
        if (len != olen)
            return false;

        // check each byte
        for (int i = 0; i < len; i++) {
            if (data[start + i] != odata[ostart + i])
                return false;
        }
        return true;
    }

    @Override
    public <R, T> R accept(IVisitablePointableVisitor<R, T> vistor, T tag) throws HyracksDataException {
        return vistor.visit(this, tag);
    }
}
