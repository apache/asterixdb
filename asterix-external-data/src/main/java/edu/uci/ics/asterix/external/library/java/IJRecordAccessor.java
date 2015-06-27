package edu.uci.ics.asterix.external.library.java;

import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IJRecordAccessor {

    public JRecord access(ARecordPointable pointable, IObjectPool<IJObject, IAType> objectPool, ARecordType recordType,
            JObjectPointableVisitor pointableVisitor) throws HyracksDataException;

}
