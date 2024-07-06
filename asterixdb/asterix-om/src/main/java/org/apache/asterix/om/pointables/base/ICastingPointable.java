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

package org.apache.asterix.om.pointables.base;

import org.apache.asterix.om.pointables.visitor.ICastingPointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;

public interface ICastingPointable extends IPointable {

    enum Type {
        FLAT,
        LIST,
        RECORD,
    }

    void setUntagged(byte[] bytes, int start, int length, ATypeTag tag);

    boolean isTagged();

    <R, T> R accept(ICastingPointableVisitor<R, T> visitor, T arg) throws HyracksDataException;

    Type getType();

    ATypeTag getTag();
}
