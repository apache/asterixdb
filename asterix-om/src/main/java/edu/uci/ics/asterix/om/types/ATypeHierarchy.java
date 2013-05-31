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
package edu.uci.ics.asterix.om.types;

import java.util.BitSet;

public class ATypeHierarchy {

    private static BitSet typeHierachyMap = new BitSet(ATypeTag.TYPE_COUNT * ATypeTag.TYPE_COUNT);

    // allow type promotion to the type itself
    static {
        for (int i = 0; i < ATypeTag.TYPE_COUNT; i++) {
            typeHierachyMap.set(i * ATypeTag.TYPE_COUNT + i);
        }
    }

    // add default type promotion rules
    static {
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT16);
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT32);
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT64);
        addPromotionRule(ATypeTag.INT16, ATypeTag.INT32);
        addPromotionRule(ATypeTag.INT16, ATypeTag.INT64);
        addPromotionRule(ATypeTag.INT32, ATypeTag.INT64);
        addPromotionRule(ATypeTag.INT8, ATypeTag.DOUBLE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.DOUBLE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.DOUBLE);
        addPromotionRule(ATypeTag.INT64, ATypeTag.DOUBLE);
        addPromotionRule(ATypeTag.FLOAT, ATypeTag.DOUBLE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.FLOAT);
        addPromotionRule(ATypeTag.INT16, ATypeTag.FLOAT);
        addPromotionRule(ATypeTag.INT32, ATypeTag.FLOAT);
    }

    public static void addPromotionRule(ATypeTag type1, ATypeTag type2) {
        typeHierachyMap.set((type1.serialize() - 1) * ATypeTag.TYPE_COUNT + (type2.serialize() - 1));
    }

    public static boolean canPromote(ATypeTag type1, ATypeTag type2) {
        return typeHierachyMap.get((type1.serialize() - 1) * ATypeTag.TYPE_COUNT + (type2.serialize() - 1));
    }

}
