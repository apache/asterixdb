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
