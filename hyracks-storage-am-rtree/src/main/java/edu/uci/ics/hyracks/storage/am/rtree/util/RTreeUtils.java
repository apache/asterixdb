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

package edu.uci.ics.hyracks.storage.am.rtree.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.FloatBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.FloatBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.DoublePrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.FloatPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.IntegerPrimitiveValueProviderFactory;

public class RTreeUtils {
    public static IPrimitiveValueProvider comparatorToPrimitiveValueProvider(IBinaryComparator cmp) {
        if (cmp instanceof IntegerBinaryComparator) {
            return IntegerPrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();
        }
        if (cmp instanceof FloatBinaryComparator) {
            return FloatPrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();
        }
        if (cmp instanceof DoubleBinaryComparator) {
            return DoublePrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();
        }
        throw new UnsupportedOperationException(
                "Converting binary comparator to primitive value provider not implemented for: " + cmp.toString());
    }

    public static IPrimitiveValueProvider[] comparatorsToPrimitiveValueProviders(IBinaryComparator[] cmps) {
        IPrimitiveValueProvider[] primitiveValueProviders = new IPrimitiveValueProvider[cmps.length];
        for (int i = 0; i < cmps.length; i++) {
            primitiveValueProviders[i] = comparatorToPrimitiveValueProvider(cmps[i]);
        }
        return primitiveValueProviders;
    }

    public static IPrimitiveValueProviderFactory comparatorToPrimitiveValueProviderFactory(IBinaryComparator cmp) {
        if (cmp instanceof IntegerBinaryComparator) {
            return IntegerPrimitiveValueProviderFactory.INSTANCE;
        }
        if (cmp instanceof FloatBinaryComparator) {
            return FloatPrimitiveValueProviderFactory.INSTANCE;
        }
        if (cmp instanceof DoubleBinaryComparator) {
            return DoublePrimitiveValueProviderFactory.INSTANCE;
        }
        throw new UnsupportedOperationException(
                "Converting binary comparator to primitive value provider factory not implemented for: "
                        + cmp.toString());
    }

    public static IPrimitiveValueProviderFactory[] comparatorsToPrimitiveValueProviderFactories(IBinaryComparator[] cmps) {
        IPrimitiveValueProviderFactory[] primitiveValueProviders = new IPrimitiveValueProviderFactory[cmps.length];
        for (int i = 0; i < cmps.length; i++) {
            primitiveValueProviders[i] = comparatorToPrimitiveValueProviderFactory(cmps[i]);
        }
        return primitiveValueProviders;
    }

    public static IPrimitiveValueProviderFactory comparatorFactoryToPrimitiveValueProviderFactory(
            IBinaryComparatorFactory cmpFactory) {
        if (cmpFactory instanceof IntegerBinaryComparatorFactory) {
            return IntegerPrimitiveValueProviderFactory.INSTANCE;
        }
        if (cmpFactory instanceof FloatBinaryComparatorFactory) {
            return FloatPrimitiveValueProviderFactory.INSTANCE;
        }
        if (cmpFactory instanceof DoubleBinaryComparatorFactory) {
            return DoublePrimitiveValueProviderFactory.INSTANCE;
        }
        throw new UnsupportedOperationException(
                "Converting binary comparator factory to primitive value provider factory not implemented for: "
                        + cmpFactory.toString());
    }

    public static IPrimitiveValueProviderFactory[] comparatorFactoriesToPrimitiveValueProviderFactories(
            IBinaryComparatorFactory[] cmpFactories) {
        IPrimitiveValueProviderFactory[] primitiveValueProviders = new IPrimitiveValueProviderFactory[cmpFactories.length];
        for (int i = 0; i < cmpFactories.length; i++) {
            primitiveValueProviders[i] = comparatorFactoryToPrimitiveValueProviderFactory(cmpFactories[i]);
        }
        return primitiveValueProviders;
    }
}
