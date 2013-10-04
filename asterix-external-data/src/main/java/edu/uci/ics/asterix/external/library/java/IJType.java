package edu.uci.ics.asterix.external.library.java;

import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ATypeTag;

public interface IJType {

      public ATypeTag getTypeTag();
      
      public IAObject getIAObject();
}
