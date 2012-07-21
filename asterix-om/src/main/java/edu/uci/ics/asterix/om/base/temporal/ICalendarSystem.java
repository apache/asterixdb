package edu.uci.ics.asterix.om.base.temporal;

public interface ICalendarSystem {

    /**
     * check whether the given time stamp is valid in the calendar system.
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param min
     * @param sec
     * @param millis
     * @param timezone
     * @return
     */
    public boolean validate(int year, int month, int day, int hour, int min, int sec, int millis, int timezone);

    /**
     * get the chronon time for the given time stamp in the calendar system.
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param min
     * @param sec
     * @param millis
     * @param timezone
     * @return
     */
    public long getChronon(int year, int month, int day, int hour, int min, int sec, int millis, int timezone);

    /**
     * get the chronon time for the given time in the calendar system
     * 
     * @param hour
     * @param min
     * @param sec
     * @param millis
     * @param timezone
     * @return
     */
    public int getChronon(int hour, int min, int sec, int millis, int timezone);

}
