/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.util;

import java.util.Properties;

public class PropertiesAccess {
    public static boolean safeBoolean(Properties props, String pName, boolean defaultValue) {
        String pValue = props.getProperty(pName);
        if (pValue == null) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(pValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static String safeString(Properties props, String pName, String defaultValue) {
        String pValue = props.getProperty(pName);
        if (pValue == null) {
            return defaultValue;
        }
        return pValue;
    }

    public static int safeInt(Properties props, String pName, int defaultValue) {
        String pValue = props.getProperty(pName);
        if (pValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(pValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}