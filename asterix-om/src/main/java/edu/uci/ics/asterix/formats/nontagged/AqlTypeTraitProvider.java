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
package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class AqlTypeTraitProvider implements ITypeTraitProvider {

    // WARNING: the byte sizes depend on the serializer!
    // currently assuming a serializer that adds a 1-byte type indicator before
    // the data
    private static final ITypeTraits ONEBYTETYPETRAIT = new TypeTrait(1 + 1);
    private static final ITypeTraits TWOBYTETYPETRAIT = new TypeTrait(2 + 1);
    private static final ITypeTraits FOURBYTETYPETRAIT = new TypeTrait(4 + 1);
    private static final ITypeTraits EIGHTBYTETYPETRAIT = new TypeTrait(8 + 1);
    private static final ITypeTraits SIXTEENBYTETYPETRAIT = new TypeTrait(16 + 1);
    private static final ITypeTraits SEVENTEENBYTETYPETRAIT = new TypeTrait(17 + 1);
    private static final ITypeTraits THIRTYTWOBYTETYPETRAIT = new TypeTrait(32 + 1);
    private static final ITypeTraits TWENTYFOURBYTETYPETRAIT = new TypeTrait(24 + 1);

    private static final ITypeTraits VARLENTYPETRAIT = new TypeTrait(false, -1);

    public static final AqlTypeTraitProvider INSTANCE = new AqlTypeTraitProvider();

    @Override
    public ITypeTraits getTypeTrait(Object type) {
        IAType aqlType = (IAType) type;
        switch (aqlType.getTypeTag()) {
            case BOOLEAN:
            case INT8:
                return ONEBYTETYPETRAIT;
            case INT16:
                return TWOBYTETYPETRAIT;
            case INT32:
            case FLOAT:
            case DATE:
            case TIME:
                return FOURBYTETYPETRAIT;
            case INT64:
            case DOUBLE:
            case DATETIME:
            case DURATION:
                return EIGHTBYTETYPETRAIT;
            case POINT:
                return SIXTEENBYTETYPETRAIT;
            case INTERVAL:
                return SEVENTEENBYTETYPETRAIT;
            case POINT3D:
                return TWENTYFOURBYTETYPETRAIT;
            case LINE:
                return THIRTYTWOBYTETYPETRAIT;
            default: {
                return VARLENTYPETRAIT;
            }
        }
    }
}

class TypeTrait implements ITypeTraits {

    @Override
    public boolean isFixedLength() {
        return isFixedLength;
    }

    @Override
    public int getFixedLength() {
        return fixedLength;
    }

    private boolean isFixedLength;
    private int fixedLength;

    public TypeTrait(boolean isFixedLength, int fixedLength) {
        this.isFixedLength = isFixedLength;
        this.fixedLength = fixedLength;
    }

    public TypeTrait(int fixedLength) {
        this.isFixedLength = true;
        this.fixedLength = fixedLength;
    }
}
