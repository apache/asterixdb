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
package org.apache.asterix.om.base;

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ARectangle implements IAObject {

    protected APoint p1;
    protected APoint p2;

    public ARectangle(APoint p1, APoint p2) {
        this.p1 = p1;
        this.p2 = p2;
    }

    public APoint getP1() {
        return p1;
    }

    public APoint getP2() {
        return p2;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ARECTANGLE;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ARectangle)) {
            return false;
        } else {
            ARectangle x = (ARectangle) obj;
            return p1.deepEqual(x.p1) && p2.deepEqual(x.p2);
        }
    }

    @Override
    public int hash() {
        return p1.hash() + 31 * p2.hash();
    }

    @Override
    public String toString() {
        return "rectangle: { p1: " + p1 + ", p2: " + p2 + "}";
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ObjectNode rectangle = om.createObjectNode();
        rectangle.set("p1", p1.toJSON());
        rectangle.set("p2", p2.toJSON());
        json.set("ARectangle", rectangle);

        return json;
    }
}
