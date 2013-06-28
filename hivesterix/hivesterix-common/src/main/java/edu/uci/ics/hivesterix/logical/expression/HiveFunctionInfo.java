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
package edu.uci.ics.hivesterix.logical.expression;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class HiveFunctionInfo implements IFunctionInfo, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * primary function identifier
     */
    private transient FunctionIdentifier fid;

    /**
     * secondary function identifier: function name
     */
    private transient Object secondaryFid;

    public HiveFunctionInfo(FunctionIdentifier fid, Object secondFid) {
        this.fid = fid;
        this.secondaryFid = secondFid;
    }

    @Override
    public FunctionIdentifier getFunctionIdentifier() {
        return fid;
    }

    public Object getInfo() {
        return secondaryFid;
    }

}
