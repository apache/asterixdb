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
package edu.uci.ics.asterix.external.dataset.adapter;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class AzureTweetEntity extends TableServiceEntity {

    private String postingType;
    private String json;

    public AzureTweetEntity() {
    }

    public AzureTweetEntity(String userID, String postingID) {
        this.partitionKey = userID;
        this.rowKey = postingID;
    }

    public String getPostingType() {
        return postingType;
    }

    public void setPostingType(String postingType) {
        this.postingType = postingType;
    }

    public void setJSON(String json) {
        this.json = json;
    }

    public String getJSON() {
        return json;
    }
}
