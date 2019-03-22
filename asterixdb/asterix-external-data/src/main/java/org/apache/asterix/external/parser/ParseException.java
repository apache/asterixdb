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
package org.apache.asterix.external.parser;

import java.io.Serializable;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ParseException extends HyracksDataException {
    private static final long serialVersionUID = 1L;
    private String filename;
    private int line = -1;
    private int column = -1;

    public ParseException(String message) {
        super(message);
    }

    public ParseException(int errorCode, Serializable... param) {
        super(ErrorCode.ASTERIX, errorCode, ErrorCode.getErrorMessage(errorCode), param);
    }

    public ParseException(int errorCode, Throwable e, Serializable... param) {
        super(ErrorCode.ASTERIX, errorCode, e, ErrorCode.getErrorMessage(errorCode), param);
        addSuppressed(e);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParseException(Throwable cause, String filename, int line, int column) {
        super(cause);
        setLocation(filename, line, column);
    }

    public void setLocation(String filename, int line, int column) {
        this.filename = filename;
        this.line = line;
        this.column = column;
    }

    @Override
    public String getMessage() {
        StringBuilder msg = new StringBuilder("Parse error");
        if (filename != null) {
            msg.append(" in file ").append(filename);
        }
        if (line >= 0) {
            msg.append(" in line ").append(line);
            if (column >= 0) {
                msg.append(", at column ").append(column);
            }
        }
        return msg.append(": ").append(super.getMessage()).toString();
    }
}
