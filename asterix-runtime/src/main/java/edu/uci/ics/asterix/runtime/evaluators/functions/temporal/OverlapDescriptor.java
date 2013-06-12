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
package edu.uci.ics.asterix.runtime.evaluators.functions.temporal;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class OverlapDescriptor extends AbstractIntervalLogicFuncDescriptor {

    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.OVERLAP;

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OverlapDescriptor();
        }
    };

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.functions.IFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.runtime.evaluators.functions.temporal.AbstractIntervalLogicFuncDescriptor#compareIntervals(long, long, long, long)
     */
    @Override
    protected boolean compareIntervals(long s1, long e1, long s2, long e2) {
        return IntervalLogic.overlap(s1, e1, s2, e2);
    }

}
