/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class ExternalScalarFunctionEvaluatorFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IExternalFunctionInfo finfo;
    private final ICopyEvaluatorFactory[] args;

    public ExternalScalarFunctionEvaluatorFactory(IExternalFunctionInfo finfo, ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        this.finfo = finfo;
        this.args = args;
    }

    @Override
    public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
        return (ExternalScalarFunction) ExternalFunctionProvider.getExternalFunctionEvaluator(finfo, args, output);
    }

}
