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
package org.apache.asterix.metadata.bootstrap;

import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;

public class MetadataBuiltinEntities {
    //--------------------------------------- Dataverses ----------------------------------------//
    public static final String DEFAULT_DATAVERSE_NAME = "Default";
    public static final Dataverse DEFAULT_DATAVERSE =
            new Dataverse(DEFAULT_DATAVERSE_NAME, NonTaggedDataFormat.class.getName(),
                    IMetadataEntity.PENDING_NO_OP);
    //--------------------------------------- Datatypes -----------------------------------------//
    public static final ARecordType ANY_OBJECT_RECORD_TYPE =
            new ARecordType("AnyObject", new String[0], new IAType[0], true);
    public static final Datatype ANY_OBJECT_DATATYPE =
            new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME, ANY_OBJECT_RECORD_TYPE.getTypeName(),
                    ARecordType.FULLY_OPEN_RECORD_TYPE, false);

    private MetadataBuiltinEntities() {
    }
}
