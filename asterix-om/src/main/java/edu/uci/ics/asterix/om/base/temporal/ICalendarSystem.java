package edu.uci.ics.asterix.om.base.temporal;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ICalendarSystem {

    public boolean validate(int year, int month, int day, int hour, int min, int sec, int millis, int timezone);

    public long getChronon(int year, int month, int day, int hour, int min, int sec, int millis, int timezone);

    public int getChronon(int hour, int min, int sec, int millis, int timezone);

}
