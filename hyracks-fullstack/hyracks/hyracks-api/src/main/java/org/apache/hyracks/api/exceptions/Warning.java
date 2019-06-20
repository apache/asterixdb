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
package org.apache.hyracks.api.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import org.apache.hyracks.api.util.ErrorMessageUtil;

public class Warning implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String component;
    private final SourceLocation srcLocation;
    private final int code;
    private final String message;

    private Warning(String component, SourceLocation srcLocation, int code, String message) {
        this.component = component;
        this.srcLocation = srcLocation;
        this.code = code;
        this.message = message;
    }

    public static Warning of(String component, SourceLocation srcLocation, int code, String message) {
        Objects.requireNonNull(srcLocation, "warnings must have source location");
        return new Warning(component, srcLocation, code, message);
    }

    public static Warning forHyracks(SourceLocation srcLocation, int code, Serializable... params) {
        return Warning.of(ErrorCode.HYRACKS, srcLocation, code, ErrorMessageUtil.formatMessage(ErrorCode.HYRACKS, code,
                ErrorCode.getErrorMessage(code), srcLocation, params));
    }

    public String getComponent() {
        return component;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public SourceLocation getSourceLocation() {
        return srcLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Warning warning = (Warning) o;
        return Objects.equals(message, warning.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }

    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(component);
        output.writeInt(code);
        output.writeUTF(message);
        srcLocation.writeFields(output);
    }

    public static Warning create(DataInput input) throws IOException {
        String comp = input.readUTF();
        int code = input.readInt();
        String msg = input.readUTF();
        return new Warning(comp, SourceLocation.create(input), code, msg);
    }

    @Override
    public String toString() {
        return "Warning{" + "component='" + component + '\'' + ", srcLocation=" + srcLocation + ", code=" + code
                + ", message='" + message + '\'' + '}';
    }
}
