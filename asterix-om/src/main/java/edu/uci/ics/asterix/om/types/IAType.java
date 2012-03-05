package edu.uci.ics.asterix.om.types;

import java.io.Serializable;

import edu.uci.ics.asterix.om.base.IAObject;

public interface IAType extends IAObject, Serializable {

    public ATypeTag getTypeTag();

    public String getDisplayName();

    public String getTypeName();

}