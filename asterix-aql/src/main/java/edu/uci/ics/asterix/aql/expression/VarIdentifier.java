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
package edu.uci.ics.asterix.aql.expression;

public final class VarIdentifier extends Identifier {
    private int id;

    public VarIdentifier() {
        super();
    }

    public VarIdentifier(String value) {
        super();
        this.value = value;
    }

    public VarIdentifier(String value, int id) {
        super();
        this.value = value;
        this.id = id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public VarIdentifier clone() {
        VarIdentifier vi = new VarIdentifier(this.value);
        vi.setId(this.id);
        return vi;
    }
}
