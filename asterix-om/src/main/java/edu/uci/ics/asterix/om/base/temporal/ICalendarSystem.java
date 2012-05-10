package edu.uci.ics.asterix.om.base.temporal;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ICalendarSystem {

    public boolean validate(int year, int month, int day, int hour, int min, int sec, int millis, int timezone);

    public long getChronon(int year, int month, int day, int hour, int min, int sec, int millis, int timezone);

    public void getStringRep(long chrononTime, StringBuilder sbder);

    public void parseStringRep(String stringRep, DataOutput out) throws HyracksDataException;

}
