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
package edu.uci.ics.hivesterix.logical.expression;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class HivesterixConstantValue implements IAlgebricksConstantValue {

    private Object object;

    public HivesterixConstantValue(Object object) {
        this.setObject(object);
    }

    @Override
    public boolean isFalse() {
        return object == Boolean.FALSE;
    }

    @Override
    public boolean isNull() {
        return object == null;
    }

    @Override
    public boolean isTrue() {
        return object == Boolean.TRUE;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Object getObject() {
        return object;
    }

    @Override
    public String toString() {
        return object.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HivesterixConstantValue)) {
            return false;
        }
        HivesterixConstantValue v2 = (HivesterixConstantValue) o;
        return object.equals(v2.getObject());
    }

    @Override
    public int hashCode() {
        return object.hashCode();
    }

}
