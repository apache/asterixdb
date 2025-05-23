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
package org.apache.asterix.lang.common.statement;

import java.util.Map;

import org.apache.asterix.om.types.ARecordType;

public class ExternalDetailsDecl implements IDatasetDetailsDecl {
    private Map<String, String> properties;
    private String adapter;
    private ARecordType itemType;
    private ARecordType parquetSchema;

    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setItemType(ARecordType itemType) {
        this.itemType = itemType;
    }

    public ARecordType getItemType() {
        return itemType;
    }

    public void setParquetSchema(ARecordType parquetSchema) {
        this.parquetSchema = parquetSchema;
    }

    public ARecordType getParquetSchema() {
        return parquetSchema;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

}
