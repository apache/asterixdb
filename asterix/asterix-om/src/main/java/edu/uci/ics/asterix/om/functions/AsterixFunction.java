/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.om.functions;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AsterixFunction {

    private final String functionName;
    private final int arity;
    public final static int VARARGS = -1;

    public AsterixFunction(String functionName, int arity) {
        this.functionName = functionName;
        this.arity = arity;
    }
    
    public AsterixFunction(FunctionIdentifier fid) {
        this.functionName = fid.getName();
        this.arity = fid.getArity();
    }

    public String getFunctionName() {
        return functionName;
    }

    public int getArity() {
        return arity;
    }

    public String toString() {
        return functionName + "@" + arity;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AsterixFunction)) {
            return false;
        }
        if (functionName.equals(((AsterixFunction) o).getFunctionName())
                && (arity == ((AsterixFunction) o).getArity() || arity == VARARGS)) {
            return true;
        }
        return false;
    }

}
