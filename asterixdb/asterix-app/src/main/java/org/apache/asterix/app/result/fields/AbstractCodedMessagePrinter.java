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
import java.util.List;

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.common.api.ICodedMessage;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.hyracks.util.JSONUtil;

public abstract class AbstractCodedMessagePrinter implements IResponseFieldPrinter {

    private enum CodedMessageField {
        CODE("code"),
        MSG("msg");

        private final String str;

        CodedMessageField(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private final List<ICodedMessage> messages;

    public AbstractCodedMessagePrinter(List<ICodedMessage> messages) {
        this.messages = messages;
    }

    @Override
    public void print(PrintWriter pw) {
        pw.print("\t\"");
        pw.print(getName());
        pw.print("\": [");
        for (int i = 0; i < messages.size(); i++) {
            final ICodedMessage codedMessage = messages.get(i);
            pw.print("{ \n\t");
            ResultUtil.printField(pw, CodedMessageField.CODE.str(), codedMessage.getCode());
            pw.print("\t");
            ResultUtil.printField(pw, CodedMessageField.MSG.str(), JSONUtil.escape(codedMessage.getMessage()), false);
            pw.print("\t} \n\t");
            boolean lastMsg = i == messages.size() - 1;
            if (!lastMsg) {
                pw.print(",");
            }
        }
        pw.print("]");
    }
}
