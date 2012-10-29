package edu.uci.ics.hyracks.control.common.job.profiling.om;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.job.JobId;

public class JobProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private final JobId jobId;

    private final Map<String, JobletProfile> jobletProfiles;

    public JobProfile(JobId jobId) {
        this.jobId = jobId;
        jobletProfiles = new HashMap<String, JobletProfile>();
    }

    public JobId getJobId() {
        return jobId;
    }

    public Map<String, JobletProfile> getJobletProfiles() {
        return jobletProfiles;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("job-id", jobId.toString());
        populateCounters(json);
        JSONArray jobletsArray = new JSONArray();
        for (JobletProfile p : jobletProfiles.values()) {
            jobletsArray.put(p.toJSON());
        }
        json.put("joblets", jobletsArray);

        return json;
    }

    public void merge(JobProfile other) {
        super.merge(this);
        for (JobletProfile jp : other.jobletProfiles.values()) {
            if (jobletProfiles.containsKey(jp.getNodeId())) {
                jobletProfiles.get(jp.getNodeId()).merge(jp);
            } else {
                jobletProfiles.put(jp.getNodeId(), jp);
            }
        }
    }
}