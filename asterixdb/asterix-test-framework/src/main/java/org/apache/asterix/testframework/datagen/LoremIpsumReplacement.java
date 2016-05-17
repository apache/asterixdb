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
package org.apache.asterix.testframework.datagen;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoremIpsumReplacement implements TemplateReplacement {

    public static final LoremIpsumReplacement INSTANCE = new LoremIpsumReplacement();

    private static final String LOREM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod "
            + "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation "
            + "ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in "
            + "voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non "
            + "proident, sunt in culpa qui officia deserunt mollit anim id est laborum. ";

    private static final Pattern p = Pattern.compile("%lorembytes:([0-9]*)%");

    private LoremIpsumReplacement() {
    }

    @Override
    public String tag() {
        return "lorembytes";
    }

    @Override
    public boolean appendReplacement(String expression, Appendable output) throws IOException {
        Matcher m = p.matcher(expression);
        if (m.find()) {
            int loremBytes = Integer.parseInt(m.group(1)) - 1;
            while (loremBytes > LOREM.length()) {
                output.append(LOREM);
                loremBytes -= LOREM.length();
            }
            output.append(LOREM, 0, loremBytes);
            return true;
        } else {
            return false;
        }
    }
}
