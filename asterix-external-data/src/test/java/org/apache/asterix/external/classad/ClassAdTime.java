/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.classad;

import java.util.Calendar;
import java.util.TimeZone;

/*
 * If not absolute, we only care about the milliseconds value
 * If absolute, we care about the milliseconds and the calendar
 */
public class ClassAdTime {

    private Calendar timeZoneCalendar;
    private boolean isAbsolute;

    public ClassAdTime(ClassAdTime t) {
        this.isAbsolute = t.isAbsolute;
        this.timeZoneCalendar = Calendar.getInstance(t.timeZoneCalendar.getTimeZone());
        this.timeZoneCalendar.setTimeInMillis(t.timeZoneCalendar.getTimeInMillis());
    }

    public TimeZone getTimeZone() {
        return timeZoneCalendar.getTimeZone();
    }

    public long getRelativeTime() {
        return timeZoneCalendar.getTimeInMillis();
    }

    // setTimeZone (parameter is in seconds)
    public void setTimeZone(int offset) {
        timeZoneCalendar.setTimeZone(TimeZone.getTimeZone(TimeZone.getAvailableIDs(offset)[0]));
        //int delta = offset - getOffsetWithDaytimeSaving();
        //timeZoneCalendar.setTimeZone(TimeZone.getTimeZone(TimeZone.getAvailableIDs(offset + delta)[0]));
    }

    // Format returned YYYY
    public int getYear() {
        return timeZoneCalendar.get(Calendar.YEAR);
    }

    // Format returned {0-11}
    public int getMonth() {
        return timeZoneCalendar.get(Calendar.MONTH);
    }

    // Format returned {1-365}
    public int getDayOfYear() {
        return timeZoneCalendar.get(Calendar.DAY_OF_YEAR);
    }

    // Format returned {1-31}
    public int getDayOfMonth() {
        return timeZoneCalendar.get(Calendar.DAY_OF_MONTH);
    }

    // Format returned {1-7}
    public int getDayOfWeek() {
        return timeZoneCalendar.get(Calendar.DAY_OF_WEEK);
    }

    // Format returned {0-23}
    public int getHours() {
        return timeZoneCalendar.get(Calendar.HOUR_OF_DAY);
    }

    // Format returned {0-59}
    public int getMinutes() {
        return timeZoneCalendar.get(Calendar.MINUTE);
    }

    // Format returned {0-59}
    public int getSeconds() {
        return timeZoneCalendar.get(Calendar.SECOND);
    }

    public void setRelativeTime(long ms) {
        this.isAbsolute = false;
        timeZoneCalendar.setTimeInMillis(ms);
    }

    // Format returned YYYY
    public void setYear(int year) {
        timeZoneCalendar.set(Calendar.YEAR, year);
    }

    // Format returned {0-11}
    public void setMonth(int month) {
        timeZoneCalendar.set(Calendar.MONTH, month);
    }

    // Format returned {1-365}
    public void setDayOfYear(int dayOfYear) {
        timeZoneCalendar.set(Calendar.DAY_OF_YEAR, dayOfYear);
    }

    // Format returned {1-31}
    public void setDayOfMonth(int day) {
        timeZoneCalendar.set(Calendar.DAY_OF_MONTH, day);
    }

    // Format returned {1-7}
    public void setDayOfWeek(int day) {
        timeZoneCalendar.set(Calendar.DAY_OF_WEEK, day);
    }

    // Format returned {0-23}
    public void setHours(int hours) {
        timeZoneCalendar.set(Calendar.HOUR_OF_DAY, hours);
    }

    // Format returned {0-59}
    public void setMinutes(int min) {
        timeZoneCalendar.set(Calendar.MINUTE, min);
    }

    // Format returned {0-59}
    public void setSeconds(int seconds) {
        timeZoneCalendar.set(Calendar.SECOND, seconds);
    }

    public ClassAdTime() {
        this.isAbsolute = true;
        this.timeZoneCalendar = Calendar.getInstance();
        this.timeZoneCalendar.setTimeInMillis(0);
    }

    public void setCurrentAbsolute() {
        this.isAbsolute = true;
        this.timeZoneCalendar = Calendar.getInstance();
        this.timeZoneCalendar.setTimeInMillis(0);
    }

    public void setTimeZone(String timeZoneId) {
        this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId));
        this.timeZoneCalendar.setTimeInMillis(0);
    }

    public ClassAdTime(String timeZoneId) {
        this.isAbsolute = true;
        this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId));
        this.timeZoneCalendar.setTimeInMillis(0);
    }

    public ClassAdTime(long ms, boolean isAbsolute) {
        this.isAbsolute = isAbsolute;
        this.timeZoneCalendar = Calendar.getInstance();
        this.timeZoneCalendar.setTimeInMillis(ms);
    }

    public ClassAdTime(boolean isAbsolute) {
        this.isAbsolute = isAbsolute;
        this.timeZoneCalendar = Calendar.getInstance();
        this.timeZoneCalendar.setTimeInMillis(System.currentTimeMillis());
    }

    //int i is in seconds
    public ClassAdTime(long ms, int i) {
        this.isAbsolute = true;
        this.timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(TimeZone.getAvailableIDs(i)[0]));
        this.timeZoneCalendar.setTimeInMillis(ms);
    }

    public void setValue(ClassAdTime t) {
        this.isAbsolute = t.isAbsolute;
        this.timeZoneCalendar.setTimeZone(t.timeZoneCalendar.getTimeZone());
        this.timeZoneCalendar.setTimeInMillis(t.timeZoneCalendar.getTimeInMillis());
    }

    public void reset() {
        this.isAbsolute = true;
        this.timeZoneCalendar.setTimeInMillis(System.currentTimeMillis());
    }

    public void makeAbsolute(boolean absolute) {
        this.isAbsolute = absolute;
    }

    public void setValue(long secs) {
        this.timeZoneCalendar.setTimeInMillis(secs);
    }

    public void setValue(long secs, boolean absolute) {
        this.isAbsolute = absolute;
        this.timeZoneCalendar.setTimeInMillis(secs);
    }

    // This probably should be double checked
    @Override
    public boolean equals(Object t) {
        if (t instanceof ClassAdTime) {
            return (((ClassAdTime) t).timeZoneCalendar.getTimeInMillis() == timeZoneCalendar.getTimeInMillis()
                    && ((ClassAdTime) t).isAbsolute == isAbsolute);
        }
        return false;
    }

    public ClassAdTime subtract(ClassAdTime t) {
        return new ClassAdTime(timeZoneCalendar.getTimeInMillis() - t.timeZoneCalendar.getTimeInMillis(), isAbsolute);
    }

    public void makeLocalAbsolute() {
        this.isAbsolute = true;
    }

    public void fromAbsoluteToRelative() {
        this.isAbsolute = false;
    }

    public void fromRelativeToAbsolute() {
        this.isAbsolute = true;
    }

    public int getOffset() {
        return timeZoneCalendar.getTimeZone().getRawOffset();//(timeZoneCalendar.getTimeInMillis());
    }

    /*
        public int getOffsetWithDaytimeSaving() {
            return timeZoneCalendar.getTimeZone().getOffset((timeZoneCalendar.getTimeInMillis()));
        }
    */
    public void setEpochTime() {
        this.timeZoneCalendar.setTimeInMillis(0);
        this.isAbsolute = true;
    }

    public ClassAdTime plus(long relativeTime, boolean absolute) {
        return new ClassAdTime(timeZoneCalendar.getTimeInMillis() + relativeTime, absolute);
    }

    public ClassAdTime subtract(ClassAdTime t, boolean b) {
        return new ClassAdTime(timeZoneCalendar.getTimeInMillis() + t.timeZoneCalendar.getTimeInMillis(), b);
    }

    public ClassAdTime multiply(long longValue, boolean b) {
        return new ClassAdTime(timeZoneCalendar.getTimeInMillis() * longValue, b);
    }

    public ClassAdTime divide(long longValue, boolean b) {
        return new ClassAdTime(timeZoneCalendar.getTimeInMillis() / longValue, b);
    }

    public void setDefaultTimeZone() {
        this.timeZoneCalendar.setTimeZone(Calendar.getInstance().getTimeZone());
    }

    public long getTimeInMillis() {
        return timeZoneCalendar.getTimeInMillis();
    }

    public void setValue(Calendar instance, ClassAdTime now) {
        this.timeZoneCalendar = instance;
        this.timeZoneCalendar.setTimeInMillis(now.getTimeInMillis());
    }

    public ClassAdTime getGMTCopy() {
        return new ClassAdTime(timeZoneCalendar.getTimeInMillis(), 0);
    }

    public long getTime() {
        return timeZoneCalendar.getTimeInMillis();
    }

    public void isAbsolute(boolean b) {
        this.isAbsolute = b;
    }

    public Calendar getCalendar() {
        return timeZoneCalendar;
    }

}
