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
package org.apache.asterix.om.base.temporal;

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
