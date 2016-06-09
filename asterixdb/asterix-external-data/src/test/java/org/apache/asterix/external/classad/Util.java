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

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.om.base.AMutableInt32;

public class Util {
    // convert escapes in-place
    // the string can only shrink while converting escapes so we can safely convert in-place.
    private static final Pattern OCTAL = Pattern.compile("\\\\([0-3][0-7]{0,2})");

    public static boolean convertEscapes(AMutableCharArrayString text) {
        boolean validStr = true;
        if (text.getLength() == 0) {
            return true;
        }
        int dest = 0;
        boolean hasOctal = false;
        for (int source = 0; source < text.getLength(); ++source) {
            char ch = text.charAt(source);
            // scan for escapes, a terminating slash cannot be an escape
            if (ch == '\\' && source < text.getLength() - 1) {
                ++source; // skip the \ character
                ch = text.charAt(source);
                // The escape part should be re-validated
                switch (ch) {
                    case 'b':
                        ch = '\b';
                        break;
                    case 'f':
                        ch = '\f';
                        break;
                    case 'n':
                        ch = '\n';
                        break;
                    case 'r':
                        ch = '\r';
                        break;
                    case 't':
                        ch = '\t';
                        break;
                    case '\\':
                        ch = '\\';
                        break;
                    default:
                        if (Lexer.isodigit(ch)) {
                            hasOctal = true;
                            ++dest;
                        }
                        break;
                }
            }

            if (dest == source) {
                // no need to assign ch to text when we haven't seen any escapes yet.
                // text[dest] = ch;
                ++dest;
            } else {
                try {
                    text.erase(dest);
                    text.setChar(dest, ch);
                    ++dest;
                    --source;
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            }
        }

        if (dest < text.getLength()) {
            text.erase(dest);
            text.setLength(dest);
        }
        // silly, but to fulfull the original contract for this function
        // we need to remove the last character in the string if it is a '\0'
        // (earlier logic guaranteed that a '\0' can ONLY be the last character)
        if (text.getLength() > 0 && (text.charAt(text.getLength() - 1) == '\0')) {
            text.erase(text.getLength() - 1);
        }
        if (hasOctal) {
            Matcher m = OCTAL.matcher(text.toString());
            StringBuffer out = new StringBuffer();
            while (m.find()) {
                int octet = Integer.parseInt(m.group(1), 8);
                if (octet == 0 || octet > 255) {
                    return false;
                }
                m.appendReplacement(out, String.valueOf((char) octet));
            }
            m.appendTail(out);
            text.setValue(new String(out.toString().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
        }

        return validStr;
    }

    public static Random initialized = new Random((new Date()).getTime());

    public static int getRandomInteger() {
        return initialized.nextInt();
    }

    public static double getRandomReal() {
        return initialized.nextDouble();
    }

    public static int timezoneOffset(ClassAdTime clock) {
        return clock.getOffset();
    }

    public static void getLocalTime(ClassAdTime now, ClassAdTime localtm) {
        localtm.setValue(Calendar.getInstance(), now);
        localtm.isAbsolute(true);
    }

    public static void absTimeToString(ClassAdTime atime, AMutableCharArrayString buffer) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        //"yyyy-MM-dd'T'HH:mm:ss"
        //2004-01-01T00:00:00+11:00
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        buffer.appendString(formatter.format(atime.getCalendar().getTime()));
        buffer.appendString(
                (atime.getOffset() >= 0 ? "+" : "-") + String.format("%02d", (Math.abs(atime.getOffset()) / 3600000))
                        + ":" + String.format("%02d", ((Math.abs(atime.getOffset() / 60) % 60))));
    }

    public static void relTimeToString(long rsecs, AMutableCharArrayString buffer) {
        double fractional_seconds;
        int days, hrs, mins;
        double secs;

        if (rsecs < 0) {
            buffer.appendChar('-');
            rsecs = -rsecs;
        }
        fractional_seconds = rsecs % 1000;

        days = (int) (rsecs / 1000);
        hrs = days % 86400;
        mins = hrs % 3600;
        secs = (mins % 60) + (fractional_seconds / 1000.0);
        days = days / 86400;
        hrs = hrs / 3600;
        mins = mins / 60;

        if (days != 0) {
            if (fractional_seconds == 0) {
                buffer.appendString(String.format("%d+%02d:%02d:%02d", days, hrs, mins, (int) secs));
            } else {
                buffer.appendString(String.format("%d+%02d:%02d:%g", days, hrs, mins, secs));
            }
        } else if (hrs != 0) {
            if (fractional_seconds == 0) {
                buffer.appendString(String.format("%02d:%02d:%02d", hrs, mins, (int) secs));
            } else {
                buffer.appendString(String.format("%02d:%02d:%02g", hrs, mins, secs));
            }
        } else if (mins != 0) {
            if (fractional_seconds == 0) {
                buffer.appendString(String.format("%02d:%02d", mins, (int) secs));
            } else {
                buffer.appendString(String.format("%02d:%02g", mins, secs));
            }
            return;
        } else {
            if (fractional_seconds == 0) {
                buffer.appendString(String.format("%02d", (int) secs));
            } else {
                buffer.appendString(String.format("%02g", secs));
            }
        }
    }

    public static void dayNumbers(int year, int month, int day, AMutableInt32 weekday, AMutableInt32 yearday) {
        int fixed = fixedFromGregorian(year, month, day);
        int jan1_fixed = fixedFromGregorian(year, 1, 1);
        weekday.setValue(fixed % 7);
        yearday.setValue(fixed - jan1_fixed);
        return;
    }

    public static int fixedFromGregorian(int year, int month, int day) {
        int fixed;
        int month_adjustment;
        if (month <= 2) {
            month_adjustment = 0;
        } else if (isLeapYear(year)) {
            month_adjustment = -1;
        } else {
            month_adjustment = -2;
        }
        fixed = 365 * (year - 1) + ((year - 1) / 4) - ((year - 1) / 100) + ((year - 1) / 400)
                + ((367 * month - 362) / 12) + month_adjustment + day;
        return fixed;
    }

    public static boolean isLeapYear(int year) {
        int mod4;
        int mod400;
        boolean leap_year;

        mod4 = year % 4;
        mod400 = year % 400;

        if (mod4 == 0 && mod400 != 100 && mod400 != 200 && mod400 != 300) {
            leap_year = true;
        } else {
            leap_year = false;
        }
        return leap_year;
    }

    public static int isInf(double x) {
        if (Double.isInfinite(x)) {
            return (x < 0.0) ? (-1) : 1;
        }
        return 0;
    }

    public static boolean isNan(double x) {
        return Double.isNaN(x);
    }
}
