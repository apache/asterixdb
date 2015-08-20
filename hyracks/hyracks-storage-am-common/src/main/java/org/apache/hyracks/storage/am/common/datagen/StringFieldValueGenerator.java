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

package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.util.Random;

public class StringFieldValueGenerator implements IFieldValueGenerator<String> {
    private int maxLen;
    private final Random rnd;
    
    public StringFieldValueGenerator(int maxLen, Random rnd) {
        this.maxLen = maxLen;
        this.rnd = rnd;
    }

    public void setMaxLength(int maxLen) {
        this.maxLen = maxLen;
    }
    
    @Override
    public String next() {
        String s = Long.toHexString(Double.doubleToLongBits(rnd.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < maxLen; i++) {
            strBuilder.append(s.charAt(Math.abs(rnd.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }

    @Override
    public void reset() {
    }
}
