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
package org.apache.asterix.lang.common.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.visitor.FormatPrintVisitor;

public class DataverseNameUtils {

    static protected Set<Character> validIdentifierChars = new HashSet<>();
    static protected Set<Character> validIdentifierStartChars = new HashSet<>();

    static {
        for (char ch = 'a'; ch <= 'z'; ++ch) {
            validIdentifierChars.add(ch);
            validIdentifierStartChars.add(ch);
        }
        for (char ch = 'A'; ch <= 'Z'; ++ch) {
            validIdentifierChars.add(ch);
            validIdentifierStartChars.add(ch);
        }
        for (char ch = '0'; ch <= '9'; ++ch) {
            validIdentifierChars.add(ch);
        }
        validIdentifierChars.add('_');
        validIdentifierChars.add('$');
    }

    protected static boolean needQuotes(String str) {
        if (str.length() == 0) {
            return false;
        }
        if (!validIdentifierStartChars.contains(str.charAt(0))) {
            return true;
        }
        for (char ch : str.toCharArray()) {
            if (!validIdentifierChars.contains(ch)) {
                return true;
            }
        }
        return false;
    }

    protected static String normalize(String str) {
        if (needQuotes(str)) {
            return FormatPrintVisitor.revertStringToQuoted(str);
        }
        return str;
    }

    public static String generateDataverseName(DataverseName dataverseName) {
        List<String> dataverseNameParts = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        dataverseNameParts.clear();
        dataverseName.getParts(dataverseNameParts);
        for (int i = 0, ln = dataverseNameParts.size(); i < ln; i++) {
            if (i > 0) {
                sb.append(DataverseName.CANONICAL_FORM_SEPARATOR_CHAR);
            }
            sb.append(normalize(dataverseNameParts.get(i)));
        }
        return sb.toString();
    }
}
