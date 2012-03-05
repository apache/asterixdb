/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.asterix.aql.base.ILiteral;

public class IntegerLiteral implements ILiteral {
    /**
     * 
     */
    private static final long serialVersionUID = -8633520244871361967L;
    private Integer value;

    public IntegerLiteral(Integer value) {
        super();
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public Type getLiteralType() {
        return Type.INTEGER;
    }

    @Override
    public String getStringValue() {
        return value.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntegerLiteral)) {
            return false;
        }
        IntegerLiteral i = (IntegerLiteral) obj;
        return value.equals(i.getValue());
    }

    @Override
    public int hashCode() {
        return value;
    }

}
