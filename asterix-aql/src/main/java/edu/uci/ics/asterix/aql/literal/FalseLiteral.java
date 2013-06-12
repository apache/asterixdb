/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.literal;

import edu.uci.ics.asterix.aql.base.Literal;

public class FalseLiteral extends Literal {

    private static final long serialVersionUID = -750814844423165149L;

    private FalseLiteral() {
    }

    public final static FalseLiteral INSTANCE = new FalseLiteral();

    @Override
    public Type getLiteralType() {
        return Type.FALSE;
    }

    @Override
    public String getStringValue() {
        return "false";
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == INSTANCE;
    }

    @Override
    public int hashCode() {
        return (int) serialVersionUID;
    }

    @Override
    public Boolean getValue() {
        return Boolean.FALSE;
    }
}
