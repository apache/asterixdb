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
package edu.uci.ics.asterix.om.types.hierachy;

import java.util.BitSet;
import java.util.HashMap;

import edu.uci.ics.asterix.om.types.ATypeTag;

public class ATypeHierarchy {

    private static BitSet typeHierachyMap = new BitSet(ATypeTag.TYPE_COUNT * ATypeTag.TYPE_COUNT);
    private static HashMap<Integer, ITypePromoteComputer> promoteComputerMap = new HashMap<Integer, ITypePromoteComputer>();

    // allow type promotion to the type itself
    static {
        for (int i = 0; i < ATypeTag.TYPE_COUNT; i++) {
            typeHierachyMap.set(i * ATypeTag.TYPE_COUNT + i);
        }
    }

    // add default type promotion rules
    static {
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT16, IntegerToInt16TypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT32, IntegerToInt32TypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.INT64, IntegerToInt64TypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.INT32, IntegerToInt32TypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.INT64, IntegerToInt64TypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.INT64, IntegerToInt64TypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.DOUBLE, IntegerToDoubleTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.DOUBLE, IntegerToDoubleTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.DOUBLE, IntegerToDoubleTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT64, ATypeTag.DOUBLE, IntegerToDoubleTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.FLOAT, ATypeTag.DOUBLE, FloatToDoubleTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT8, ATypeTag.FLOAT, IntegerToFloatTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT16, ATypeTag.FLOAT, IntegerToFloatTypePromoteComputer.INSTANCE);
        addPromotionRule(ATypeTag.INT32, ATypeTag.FLOAT, IntegerToFloatTypePromoteComputer.INSTANCE);
    }

    public static void addPromotionRule(ATypeTag type1, ATypeTag type2, ITypePromoteComputer promoteComputer) {
        int index = type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal();
        typeHierachyMap.set(index);
        promoteComputerMap.put(index, promoteComputer);
    }

    public static ITypePromoteComputer getTypePromoteComputer(ATypeTag type1, ATypeTag type2) {
        if (canPromote(type1, type2)) {
            return promoteComputerMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
        }
        return null;
    }

    public static boolean canPromote(ATypeTag type1, ATypeTag type2) {
        return typeHierachyMap.get(type1.ordinal() * ATypeTag.TYPE_COUNT + type2.ordinal());
    }

    public static boolean isCompatible(ATypeTag type1, ATypeTag type2) {
        return canPromote(type1, type2) | canPromote(type2, type1);
    }

}
