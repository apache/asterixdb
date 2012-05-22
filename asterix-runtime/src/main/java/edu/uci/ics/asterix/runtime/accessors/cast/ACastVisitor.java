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

package edu.uci.ics.asterix.runtime.accessors.cast;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.accessors.AFlatValueAccessor;
import edu.uci.ics.asterix.runtime.accessors.AListAccessor;
import edu.uci.ics.asterix.runtime.accessors.ARecordAccessor;
import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;
import edu.uci.ics.asterix.runtime.accessors.visitor.IBinaryAccessorVisitor;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;

public class ACastVisitor implements IBinaryAccessorVisitor<Void, Triple<IBinaryAccessor, IAType, Boolean>> {

    private Map<IBinaryAccessor, ARecordCaster> raccessorToCaster = new HashMap<IBinaryAccessor, ARecordCaster>();
    private Map<IBinaryAccessor, AListCaster> laccessorToCaster = new HashMap<IBinaryAccessor, AListCaster>();

    @Override
    public Void visit(AListAccessor accessor, Triple<IBinaryAccessor, IAType, Boolean> arg) throws AsterixException {
        AListCaster caster = laccessorToCaster.get(accessor);
        if (caster == null) {
            caster = new AListCaster();
            laccessorToCaster.put(accessor, caster);
        }
        try {
            caster.castList(accessor, arg.first, (AbstractCollectionType) arg.second, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return null;
    }

    @Override
    public Void visit(ARecordAccessor accessor, Triple<IBinaryAccessor, IAType, Boolean> arg) throws AsterixException {
        ARecordCaster caster = raccessorToCaster.get(accessor);
        if (caster == null) {
            caster = new ARecordCaster();
            raccessorToCaster.put(accessor, caster);
        }
        try {
            caster.castRecord(accessor, arg.first, (ARecordType) arg.second, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return null;
    }

    @Override
    public Void visit(AFlatValueAccessor accessor, Triple<IBinaryAccessor, IAType, Boolean> arg) {
        // set the pointer for result
        arg.first.reset(accessor.getBytes(), accessor.getStartIndex(), accessor.getLength());
        return null;
    }

}
