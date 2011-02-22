package edu.uci.ics.hyracks.control.common.job.profiling.om;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

public class JobProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private final UUID jobId;

    private final int attempt;

    private final Map<String, JobletProfile> jobletProfiles;

    public JobProfile(UUID jobId, int attempt) {
        this.jobId = jobId;
        this.attempt = attempt;
        jobletProfiles = new HashMap<String, JobletProfile>();
    }

    public UUID getJobId() {
        return jobId;
    }

    public int getAttempt() {
        return attempt;
    }

    public Map<String, JobletProfile> getJobletProfiles() {
        return jobletProfiles;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("type", "job-profile");
        json.put("job-id", jobId.toString());
        json.put("attempt", attempt);
        populateCounters(json);
        for (JobletProfile p : jobletProfiles.values()) {
            json.accumulate("joblets", p.toJSON());
        }

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