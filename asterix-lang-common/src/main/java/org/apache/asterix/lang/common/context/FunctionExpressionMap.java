/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.lang.common.context;

import java.util.HashMap;

import org.apache.asterix.common.functions.FunctionSignature;

public class FunctionExpressionMap extends HashMap<Integer, FunctionSignature> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private boolean varargs;

    public boolean isVarargs() {
        return varargs;
    }

    public void setVarargs(boolean varargs) {
        this.varargs = varargs;
    }

    public FunctionExpressionMap(boolean varargs) {
        super();
        this.varargs = varargs;
    }

    public FunctionSignature get(int arity) {
        if (varargs) {
            return super.get(-1);
        } else {
            return super.get(arity);
        }
    }

    public FunctionSignature put(int arity, FunctionSignature fd) {
        if (varargs) {
            return super.put(-1, fd);
        } else {
            return super.put(arity, fd);
        }
    }
}
