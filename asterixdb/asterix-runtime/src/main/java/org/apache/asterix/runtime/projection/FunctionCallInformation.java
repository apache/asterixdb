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
package org.apache.asterix.runtime.projection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

/**
 * Function call information that holds {@link FunctionIdentifier#getName()} and {@link SourceLocation}
 */
public class FunctionCallInformation implements Serializable {
    private static final long serialVersionUID = -7884346933746232736L;
    private final String functionName;
    private final SourceLocation sourceLocation;
    private final IProjectionFiltrationWarningFactory warningFactory;
    private Set<ATypeTag> typeMismatches;

    public FunctionCallInformation(String functionName, SourceLocation sourceLocation,
            IProjectionFiltrationWarningFactory warningFactory) {
        this(functionName, sourceLocation, Collections.emptySet(), warningFactory);
    }

    private FunctionCallInformation(String functionName, SourceLocation sourceLocation, Set<ATypeTag> typeMismatches,
            IProjectionFiltrationWarningFactory warningFactory) {
        this.functionName = functionName;
        this.sourceLocation = sourceLocation;
        this.typeMismatches = typeMismatches;
        this.warningFactory = warningFactory;
    }

    public String getFunctionName() {
        return functionName;
    }

    public SourceLocation getSourceLocation() {
        return sourceLocation;
    }

    public Warning createWarning(ATypeTag expectedType, ATypeTag actualType) {
        if (typeMismatches.isEmpty()) {
            typeMismatches = EnumSet.noneOf(ATypeTag.class);
        } else if (typeMismatches.contains(actualType)) {
            //We already issued a warning containing the same actual type. So, we ignore it
            return null;
        }
        typeMismatches.add(actualType);
        return warningFactory.createWarning(getSourceLocation(), getFunctionName(), ExceptionUtil.indexToPosition(0),
                expectedType, actualType);
    }

    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(functionName);
        SourceLocation.writeFields(sourceLocation, output);
        output.writeInt(typeMismatches.size());
        for (ATypeTag typeTag : typeMismatches) {
            output.write(typeTag.serialize());
        }
    }

    public static FunctionCallInformation create(DataInput in) throws IOException {
        String functionName = in.readUTF();
        SourceLocation sourceLocation = SourceLocation.create(in);
        int typeMismatchesLength = in.readInt();
        Set<ATypeTag> warnings = EnumSet.noneOf(ATypeTag.class);
        IProjectionFiltrationWarningFactory warningFactory =
                ProjectionFiltrationWarningFactoryProvider.TYPE_MISMATCH_FACTORY;
        for (int i = 0; i < typeMismatchesLength; i++) {
            warnings.add(ATypeTag.VALUE_TYPE_MAPPING[in.readByte()]);
        }
        return new FunctionCallInformation(functionName, sourceLocation, warnings, warningFactory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, sourceLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionCallInformation that = (FunctionCallInformation) o;
        return Objects.equals(functionName, that.functionName) && Objects.equals(sourceLocation, that.sourceLocation);
    }
}
