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
package edu.uci.ics.asterix.external.library.java;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.TypeInfo;
import edu.uci.ics.asterix.external.library.java.JObjectAccessors.JListAccessor;
import edu.uci.ics.asterix.external.library.java.JObjectAccessors.JRecordAccessor;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListVisitablePointable;
import edu.uci.ics.asterix.om.pointables.ARecordVisitablePointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class JObjectPointableVisitor implements IVisitablePointableVisitor<IJObject, TypeInfo> {

    private final Map<ATypeTag, IJObjectAccessor> flatJObjectAccessors = new HashMap<ATypeTag, IJObjectAccessor>();
    private final Map<IVisitablePointable, IJRecordAccessor> raccessorToJObject = new HashMap<IVisitablePointable, IJRecordAccessor>();
    private final Map<IVisitablePointable, IJListAccessor> laccessorToPrinter = new HashMap<IVisitablePointable, IJListAccessor>();

    @Override
    public IJObject visit(AListVisitablePointable accessor, TypeInfo arg) throws AsterixException {
        IJObject result = null;
        IJListAccessor jListAccessor = laccessorToPrinter.get(accessor);
        if (jListAccessor == null) {
            jListAccessor = new JListAccessor(arg.getObjectPool());
            laccessorToPrinter.put(accessor, jListAccessor);
        }
        try {
            result = jListAccessor.access(accessor, arg.getObjectPool(), arg.getAtype(), this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return result;
    }

    @Override
    public IJObject visit(ARecordVisitablePointable accessor, TypeInfo arg) throws AsterixException {
        IJObject result = null;
        IJRecordAccessor jRecordAccessor = raccessorToJObject.get(accessor);
        if (jRecordAccessor == null) {
            jRecordAccessor = new JRecordAccessor(accessor.getInputRecordType(), arg.getObjectPool());
            raccessorToJObject.put(accessor, jRecordAccessor);
        }
        try {
            result = jRecordAccessor.access(accessor, arg.getObjectPool(), (ARecordType) arg.getAtype(), this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return result;
    }

    @Override
    public IJObject visit(AFlatValuePointable accessor, TypeInfo arg) throws AsterixException {
        ATypeTag typeTag = arg.getTypeTag();
        IJObject result = null;
        IJObjectAccessor jObjectAccessor = flatJObjectAccessors.get(typeTag);
        if (jObjectAccessor == null) {
            jObjectAccessor = JObjectAccessors.createFlatJObjectAccessor(typeTag);
            flatJObjectAccessors.put(typeTag, jObjectAccessor);
        }

        try {
            result = jObjectAccessor.access(accessor, arg.getObjectPool());
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
        return result;
    }

}
