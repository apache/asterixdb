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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.om.base.AMutableLine;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JLine extends JObject {

    public JLine(JPoint p1, JPoint p2) {
        super(new AMutableLine((APoint) p1.getIAObject(), (APoint) p2.getIAObject()));
    }

    public void setValue(JPoint p1, JPoint p2) {
        ((AMutableLine) value).setValue((APoint) p1.getIAObject(), (APoint) p2.getIAObject());
    }

    public void setValue(APoint p1, APoint p2) {
        ((AMutableLine) value).setValue(p1, p2);
    }

    public Pair<Double, Double> getBeginPoint() {
        return Pair.of(((AMutableLine) value).getP1().getX(), ((AMutableLine) value).getP1().getY());
    }

    public Pair<Double, Double> getEndPoint() {
        return Pair.of(((AMutableLine) value).getP2().getX(), ((AMutableLine) value).getP2().getY());
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.LINE);
        ALineSerializerDeserializer.INSTANCE.serialize((AMutableLine) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutableLine) value).setValue(null, null);
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.ALINE;
    }
}
