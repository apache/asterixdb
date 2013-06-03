/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.util.regex.Pattern;

import edu.uci.ics.asterix.om.base.AString;

public class StringEvaluatorUtils {

    public static int toFlag(AString pattern) {
        String str = pattern.getStringValue();
        int flag = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case 's':
                    flag |= Pattern.DOTALL;
                    break;
                case 'm':
                    flag |= Pattern.MULTILINE;
                    break;
                case 'i':
                    flag |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'x':
                    flag |= Pattern.COMMENTS;
                    break;
            }
        }
        return flag;
    }

    public final static char[] reservedRegexChars = new char[] { '$', '(', ')', '*', '.', '[', '\\', ']', '^', '{',
            '|', '}' };
    
}
