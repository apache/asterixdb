/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.asterix.runtime.exceptions;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class UnsupportedItemTypeException extends RuntimeDataException {
    private static final long serialVersionUID = -3443141044058396191L;

    // Unsupported item type.
    public UnsupportedItemTypeException(SourceLocation sourceLoc, FunctionIdentifier fid, byte itemTypeTag) {
        super(ErrorCode.TYPE_ITEM, sourceLoc, fid.getName(),
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(itemTypeTag));
    }

    // Unsupported item type.
    public UnsupportedItemTypeException(SourceLocation sourceLoc, String functionName, byte itemTypeTag) {
        super(ErrorCode.TYPE_ITEM, sourceLoc, functionName,
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(itemTypeTag));
    }
}
