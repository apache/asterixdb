/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.runtime.pointables;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.runtime.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.runtime.util.container.IElementFactory;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class AFlatValuePointable extends AbstractVisitablePointable {

    public static IElementFactory<IVisitablePointable, IAType> FACTORY = new IElementFactory<IVisitablePointable, IAType>() {
        public AFlatValuePointable createElement(IAType type) {
            return new AFlatValuePointable();
        }
    };

    private AFlatValuePointable() {

    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IValueReference))
            return false;
        IValueReference ivf = (IValueReference) o;
        byte[] odata = ivf.getByteArray();
        int ostart = ivf.getStartOffset();
        int olen = ivf.getLength();

        byte[] data = getByteArray();
        int start = getStartOffset();
        int len = getLength();
        if (len != olen)
            return false;
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
