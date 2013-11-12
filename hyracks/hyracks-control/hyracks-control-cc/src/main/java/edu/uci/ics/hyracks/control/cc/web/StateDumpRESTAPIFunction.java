package edu.uci.ics.hyracks.control.cc.web;

import java.util.Map;

import org.json.JSONObject;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.web.util.IJSONOutputFunction;
import edu.uci.ics.hyracks.control.cc.work.GatherStateDumpsWork;
import edu.uci.ics.hyracks.control.cc.work.GatherStateDumpsWork.StateDumpRun;

public class StateDumpRESTAPIFunction implements IJSONOutputFunction {
    private final ClusterControllerService ccs;

    public StateDumpRESTAPIFunction(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public JSONObject invoke(String[] arguments) throws Exception {
        GatherStateDumpsWork gsdw = new GatherStateDumpsWork(ccs);
        ccs.getWorkQueue().scheduleAndSync(gsdw);
        StateDumpRun sdr = gsdw.getStateDumpRun();
        sdr.waitForCompletion();

        JSONObject result = new JSONObject();
        for (Map.Entry<String, String> e : sdr.getStateDump().entrySet()) {
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }

}
