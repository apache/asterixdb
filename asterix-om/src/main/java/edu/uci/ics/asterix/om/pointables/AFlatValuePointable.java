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

package edu.uci.ics.asterix.om.pointables;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectFactory;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

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
    static IObjectFactory<IVisitablePointable, IAType> FACTORY = new IObjectFactory<IVisitablePointable, IAType>() {
        public AFlatValuePointable create(IAType type) {
            return new AFlatValuePointable();
        }
    };

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
    public <R, T> R accept(IVisitablePointableVisitor<R, T> vistor, T tag) throws AsterixException {
        return vistor.visit(this, tag);
    }
}
