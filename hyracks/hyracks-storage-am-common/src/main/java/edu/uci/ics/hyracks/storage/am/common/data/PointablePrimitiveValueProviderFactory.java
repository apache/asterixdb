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
package edu.uci.ics.hyracks.storage.am.common.data;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;

public class PointablePrimitiveValueProviderFactory implements IPrimitiveValueProviderFactory {
    private static final long serialVersionUID = 1L;

    private final IPointableFactory pf;

    public PointablePrimitiveValueProviderFactory(IPointableFactory pf) {
        this.pf = pf;
    }

    @Override
    public IPrimitiveValueProvider createPrimitiveValueProvider() {
        final IPointable p = pf.createPointable();
        ITypeTraits traits = pf.getTypeTraits();
        assert traits.isFixedLength();
        final int length = traits.getFixedLength();
        return new IPrimitiveValueProvider() {
            @Override
            public double getValue(byte[] bytes, int offset) {
                p.set(bytes, offset, length);
                return ((INumeric) p).doubleValue();
            }
        };
    }
}