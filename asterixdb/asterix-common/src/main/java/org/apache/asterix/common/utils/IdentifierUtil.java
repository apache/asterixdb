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

package org.apache.asterix.common.utils;

import static org.apache.asterix.common.api.IIdentifierMapper.Modifier;
import static org.apache.asterix.common.api.IIdentifierMapper.Modifier.NONE;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.api.IIdentifierMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IdentifierUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Pattern MESSAGE_IDENTIFIERS = Pattern.compile("@([A-Z_]*)(:([A-Z_]*))?@");

    public static final String DATASET = "DATASET";
    public static final String DATAVERSE = "DATAVERSE";
    public static final String PRODUCT_NAME = "PRODUCT_NAME";
    public static final String PRODUCT_ABBREVIATION = "PRODUCT_ABBREVIATION";

    public static String dataset() {
        return IdentifierMappingUtil.map(DATASET, NONE);
    }

    public static String dataset(Modifier modifier) {
        return IdentifierMappingUtil.map(DATASET, modifier);
    }

    public static String dataverse() {
        return IdentifierMappingUtil.map(DATAVERSE, NONE);
    }

    public static String productName() {
        return IdentifierMappingUtil.map(PRODUCT_NAME, NONE);
    }

    public static String productAbbreviation() {
        return IdentifierMappingUtil.map(PRODUCT_ABBREVIATION, NONE);
    }

    public static String replaceIdentifiers(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        Matcher m = MESSAGE_IDENTIFIERS.matcher(input);
        String replacement = m.replaceAll(mr -> {
            String identifier = mr.group(1);
            String modifierStr = mr.group(3);
            IIdentifierMapper.Modifier modifier;
            if (modifierStr != null) {
                modifier = IIdentifierMapper.Modifier.valueOf(modifierStr);
            } else {
                modifier = IIdentifierMapper.Modifier.NONE;
            }
            return IdentifierMappingUtil.map(identifier, modifier);
        });
        if (!input.equals(replacement)) {
            LOGGER.debug("{} -> {}", input, replacement);
        }
        return replacement;
    }

    public static String[] replaceIdentifiers(String[] input) {
        for (int i = 0; i < input.length; i++) {
            input[i] = IdentifierUtil.replaceIdentifiers(input[i]);
        }
        return input;
    }
}
