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

package org.apache.asterix.om.functions;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public final class BuiltinFunctionInfo extends FunctionInfo {
    private static final long serialVersionUID = -6013109889177637590L;

    private final boolean isPrivate;

    public BuiltinFunctionInfo(FunctionIdentifier fi, IResultTypeComputer typeComputer, boolean isFunctional,
            boolean isPrivate) {
        super(fi, typeComputer, isFunctional);
        this.isPrivate = isPrivate;
    }

    public boolean isPrivate() {
        return isPrivate;
    }
}
