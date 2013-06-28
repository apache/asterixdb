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
package edu.uci.ics.asterix.om.functions;

import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;

public class AsterixExternalFunctionInfo extends AsterixFunctionInfo implements IExternalFunctionInfo {

    private final IResultTypeComputer rtc;
    private final List<IAType> argumentTypes;
    private final String body;
    private final String language;
    private final FunctionKind kind;
    private final IAType returnType;

    public AsterixExternalFunctionInfo(){
        super();
        rtc = null;
        argumentTypes= null;
        body = null;
        language=null;
        kind = null;
        returnType = null;
                
    }
    
    public AsterixExternalFunctionInfo(String namespace, AsterixFunction asterixFunction, FunctionKind kind,
            List<IAType> argumentTypes, IAType returnType, IResultTypeComputer rtc, String body, String language) {
        super(namespace, asterixFunction);
        this.rtc = rtc;
        this.argumentTypes = argumentTypes;
        this.body = body;
        this.language = language;
        this.kind = kind;
        this.returnType = returnType;
    }

    public IResultTypeComputer getResultTypeComputer() {
        return rtc;
    }

    public List<IAType> getArgumenTypes() {
        return argumentTypes;
    }

    @Override
    public String getFunctionBody() {
        return body;
    }

    @Override
    public List<IAType> getParamList() {
        return argumentTypes;
    }

    @Override
    public String getLanguage() {
        return language;
    }

    @Override
    public FunctionKind getKind() {
        return kind;
    }

    @Override
    public IAType getReturnType() {
        return returnType;
    }

}
