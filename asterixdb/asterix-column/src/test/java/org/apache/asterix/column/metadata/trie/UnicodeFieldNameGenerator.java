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
package org.apache.asterix.column.metadata.trie;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class UnicodeFieldNameGenerator {

    // Unicode ranges for different scripts and symbols
    private static final char[] LATIN_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final char[] NUMBER_CHARS = "0123456789".toCharArray();
    private static final char[] GREEK_CHARS = "αβγδεζηθικλμνξοπρστυφχψωΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩ".toCharArray();
    private static final char[] CYRILLIC_CHARS =
            "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ".toCharArray();
    private static final char[] CHINESE_CHARS =
            "的一是了我不人在他有这个上们来到时大地为子中你说生国年着就那和要她出也得里后自以会家可下而过天去能对小多然于心学好".toCharArray();
    private static final char[] JAPANESE_CHARS =
            "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをんアイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン"
                    .toCharArray();
    private static final char[] EMOJIS = "😀😁😂🤣😃😄😅😆😉😊😋😎😍😘".toCharArray();

    private static final char[][] UNICODE_RANGES =
            { LATIN_CHARS, NUMBER_CHARS, GREEK_CHARS, CYRILLIC_CHARS, CHINESE_CHARS, JAPANESE_CHARS, EMOJIS };
    private static final Random RANDOM = new Random();

    public static void genFields(int numFields, int minLength, int maxLength, int biasMaxLength, float bias,
            String fileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            int biasedCount = (int) (numFields * bias);
            int remainingCount = numFields - biasedCount;

            for (int i = 0; i < biasedCount; i++) {
                int length = RANDOM.nextInt(biasMaxLength - minLength + 1) + minLength;
                String word = generateFieldName(length);
                writer.write(word);
                writer.newLine();
            }

            for (int i = 0; i < remainingCount; i++) {
                int length = RANDOM.nextInt(maxLength - biasMaxLength + 1) + biasMaxLength;
                String word = generateFieldName(length);
                writer.write(word);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String generateFieldName(int length) {
        StringBuilder fieldName = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char[] selectedRange = UNICODE_RANGES[RANDOM.nextInt(UNICODE_RANGES.length)];
            char randomChar = selectedRange[RANDOM.nextInt(selectedRange.length)];
            fieldName.append(randomChar);
        }
        return fieldName.toString();
    }

}
