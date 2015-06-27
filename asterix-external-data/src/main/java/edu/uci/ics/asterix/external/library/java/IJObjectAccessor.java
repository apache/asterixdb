package edu.uci.ics.asterix.external.library.java;

import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IJObjectAccessor {
    IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> obj) throws HyracksDataException;

}
