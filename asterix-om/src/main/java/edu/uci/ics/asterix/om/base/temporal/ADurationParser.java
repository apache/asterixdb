/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.om.base.temporal;

import edu.uci.ics.asterix.om.base.AMutableDuration;

public class ADurationParser {

    private enum State {
        NOTHING_READ,
        YEAR,
        MONTH,
        DAY,
        TIME,
        HOUR,
        MIN,
        MILLISEC,
        SEC;
    };

    private static final String errorMessage = "This can not be an instance of duration";

    public static <T> void parse(ICharSequenceAccessor<T> charAccessor, AMutableDuration aDuration) throws Exception {

        boolean positive = true;
        int offset = 0;
        int value = 0, hour = 0, minute = 0, second = 0, millisecond = 0, year = 0, month = 0, day = 0;
        State state = State.NOTHING_READ;

        if (charAccessor.getCharAt(offset) == '-') {
            offset++;
            positive = false;
        }

        if (charAccessor.getCharAt(offset++) != 'P') {
            throw new Exception(errorMessage);
        }

        for (; offset < charAccessor.getLength(); offset++) {
            if (charAccessor.getCharAt(offset) >= '0' && charAccessor.getCharAt(offset) <= '9') {
                // accumulate the digit fields
                value = value * 10 + charAccessor.getCharAt(offset) - '0';
            } else {
                switch (charAccessor.getCharAt(offset)) {
                    case 'Y':
                        if (state.compareTo(State.YEAR) < 0) {
                            year = value;
                            state = State.YEAR;
                        } else {
                            throw new Exception(errorMessage);
                        }
                        break;
                    case 'M':
                        if (state.compareTo(State.TIME) < 0) {
                            if (state.compareTo(State.MONTH) < 0) {
                                month = value;
                                state = State.MONTH;
                            } else {
                                throw new Exception(errorMessage);
                            }
                        } else if (state.compareTo(State.MIN) < 0) {
                            minute = value;
                            state = State.MIN;
                        } else {
                            throw new Exception(errorMessage);
                        }
                        break;
                    case 'D':
                        if (state.compareTo(State.DAY) < 0) {
                            day = value;
                            state = State.DAY;
                        } else {
                            throw new Exception(errorMessage);
                        }
                        break;
                    case 'T':
                        if (state.compareTo(State.TIME) < 0) {
                            state = State.TIME;
                        } else {
                            throw new Exception(errorMessage);
                        }
                        break;

                    case 'H':
                        if (state.compareTo(State.HOUR) < 0) {
                            hour = value;
                            state = State.HOUR;
                        } else {
                            throw new Exception(errorMessage);
                        }
                        break;
                    case '.':
                        if (state.compareTo(State.MILLISEC) < 0) {
                            int i = 1;
                            for (; offset + i < charAccessor.getLength(); i++) {
                                if (charAccessor.getCharAt(offset + i) >= '0'
                                        && charAccessor.getCharAt(offset + i) <= '9') {
                                    if (i < 4) {
                                        millisecond = millisecond * 10 + (charAccessor.getCharAt(offset + i) - '0');
                                    } else {
                                        throw new Exception(errorMessage);
                                    }
                                } else {
                                    break;
                                }
                            }
                            offset += i;
                            state = State.MILLISEC;
                        } else {
                            throw new Exception(errorMessage);
                        }
                    case 'S':
                        if (state.compareTo(State.SEC) < 0) {
                            second = value;
                            state = State.SEC;
                        } else {
                            throw new Exception(errorMessage);
                        }
                        break;
                    default:
                        throw new Exception(errorMessage);

                }
                value = 0;
            }
        }

        if (state.compareTo(State.TIME) == 0) {
            throw new Exception(errorMessage);
        }

        short temp = 1;
        if (!positive) {
            temp = -1;
        }

        aDuration.setValue(temp * (year * 12 + month), temp
                * (day * 24 * 3600 * 1000L + 3600 * 1000L * hour + 60 * minute * 1000L + second * 1000L + millisecond));

    }
}
