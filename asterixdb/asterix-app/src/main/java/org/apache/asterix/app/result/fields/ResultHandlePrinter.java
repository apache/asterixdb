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
package org.apache.asterix.app.result.fields;

import java.io.PrintWriter;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ResultHandlePrinter implements IResponseFieldPrinter {

    public static final String FIELD_NAME = "handle";
    private final SessionOutput sessionOutput;
    private final String handle;

    public ResultHandlePrinter(SessionOutput sessionOutput, ResultHandle handle) {
        this.sessionOutput = sessionOutput;
        this.handle = handle.toString();
    }

    public ResultHandlePrinter(String handle) {
        this.handle = handle;
        sessionOutput = null;
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        if (sessionOutput != null) {
            final AlgebricksAppendable app = new AlgebricksAppendable(pw);
            try {
                sessionOutput.appendHandle(app, handle);
            } catch (AlgebricksException e) {
                throw HyracksDataException.create(e);
            }
        } else {
            ResultUtil.printField(pw, FIELD_NAME, handle, false);
        }
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
