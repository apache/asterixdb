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
package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;

public class TypeInfo {

    private IObjectPool<IJObject, IAType> objectPool;
    private IAType atype;
    private ATypeTag typeTag;

    public TypeInfo(IObjectPool<IJObject, IAType> objectPool, IAType atype, ATypeTag typeTag) {
        this.objectPool = objectPool;
        this.atype = atype;
        this.typeTag = typeTag;
    }

    public void reset(IAType atype, ATypeTag typeTag) {
        this.atype = atype;
        this.typeTag = typeTag;
    }

    public IObjectPool<IJObject, IAType> getObjectPool() {
        return objectPool;
    }

    public void setObjectPool(IObjectPool<IJObject, IAType> objectPool) {
        this.objectPool = objectPool;
    }

    public IAType getAtype() {
        return atype;
    }

    public void setAtype(IAType atype) {
        this.atype = atype;
    }

    public ATypeTag getTypeTag() {
        return typeTag;
    }

    public void setTypeTag(ATypeTag typeTag) {
        this.typeTag = typeTag;
    }

}
