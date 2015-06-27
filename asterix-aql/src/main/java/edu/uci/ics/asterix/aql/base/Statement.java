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
package edu.uci.ics.asterix.aql.base;

public interface Statement extends IAqlExpression {
    public enum Kind {
        DATASET_DECL,
        DATAVERSE_DECL,
        DATAVERSE_DROP,
        DATASET_DROP,
        DELETE,
        INSERT,
        UPDATE,
        DML_CMD_LIST,
        FUNCTION_DECL,
        LOAD,
        NODEGROUP_DECL,
        NODEGROUP_DROP,
        QUERY,
        SET,
        TYPE_DECL,
        TYPE_DROP,
        WRITE,
        CREATE_INDEX,
        INDEX_DECL,
        CREATE_DATAVERSE,
        INDEX_DROP,
        CREATE_PRIMARY_FEED,
        CREATE_SECONDARY_FEED,
        DROP_FEED,
        CONNECT_FEED,
        DISCONNECT_FEED,
        SUBSCRIBE_FEED,
        CREATE_FEED_POLICY,
        DROP_FEED_POLICY,
        CREATE_FUNCTION,
        FUNCTION_DROP,
        COMPACT, 
        EXTERNAL_DATASET_REFRESH,
        RUN
    }

    public abstract Kind getKind();

}
