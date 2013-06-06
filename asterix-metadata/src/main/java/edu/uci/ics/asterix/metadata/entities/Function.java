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
package edu.uci.ics.asterix.metadata.entities;

import java.util.List;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

public class Function implements IMetadataEntity {

    public static final String LANGUAGE_AQL = "AQL";
    public static final String LANGUAGE_JAVA = "JAVA";

    public static final String RETURNTYPE_VOID = "VOID";
    public static final String NOT_APPLICABLE = "N/A";

    private final String dataverse;
    private final String name;
    private final int arity;
    private final List<String> params;
    private final String body;
    private final String returnType;
    private final String language;
    private final String kind;

    public Function(String dataverseName, String functionName, int arity, List<String> params, String returnType,
            String functionBody, String language, String functionKind) {
        this.dataverse = dataverseName;
        this.name = functionName;
        this.params = params;
        this.body = functionBody;
        this.returnType = returnType == null ? RETURNTYPE_VOID : returnType;
        this.language = language;
        this.kind = functionKind;
        this.arity = arity;
    }

    public String getDataverseName() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

    public List<String> getParams() {
        return params;
    }

    public String getFunctionBody() {
        return body;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public int getArity() {
        return arity;
    }

    public String getKind() {
        return kind;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addFunctionIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropFunction(this);
    }

}
