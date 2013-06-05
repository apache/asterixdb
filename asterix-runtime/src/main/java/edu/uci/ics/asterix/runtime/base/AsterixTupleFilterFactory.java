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

package edu.uci.ics.asterix.runtime.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilter;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;

public class AsterixTupleFilterFactory implements ITupleFilterFactory {

    private static final long serialVersionUID = 1L;

    private final IBinaryBooleanInspectorFactory boolInspectorFactory;
    private final IScalarEvaluatorFactory evalFactory;

    public AsterixTupleFilterFactory(IScalarEvaluatorFactory evalFactory,
            IBinaryBooleanInspectorFactory boolInspectorFactory) throws AlgebricksException {
        this.evalFactory = evalFactory;
        this.boolInspectorFactory = boolInspectorFactory;
    }

    @Override
    public ITupleFilter createTupleFilter(IHyracksTaskContext ctx) throws Exception {
        return new AsterixTupleFilter(ctx, evalFactory, boolInspectorFactory.createBinaryBooleanInspector(ctx));
    }

}
