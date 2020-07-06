/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.asterix.common.library;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The information needed to libraries at startup
 */
public class LibraryDescriptor implements IJsonSerializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_LANGUAGE = "lang";

    public static final String FILE_EXT_ZIP = "zip";

    public static final String FILE_EXT_PYZ = "pyz";

    /**
     * The library's language
     */
    private final ExternalFunctionLanguage lang;

    public LibraryDescriptor(ExternalFunctionLanguage language) {
        this.lang = language;
    }

    public ExternalFunctionLanguage getLanguage() {
        return lang;
    }

    public JsonNode toJson(IPersistedResourceRegistry registry) {
        ObjectNode jsonNode = registry.getClassIdentifier(LibraryDescriptor.class, serialVersionUID);
        jsonNode.put(FIELD_LANGUAGE, lang.name());
        return jsonNode;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        String langText = json.get(FIELD_LANGUAGE).asText();
        ExternalFunctionLanguage lang = ExternalFunctionLanguage.valueOf(langText);
        return new LibraryDescriptor(lang);
    }
}
