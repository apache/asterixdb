package edu.uci.ics.hyracks.adminconsole.client.details.node.charts;

import com.google.gwt.core.client.JsArrayNumber;
import com.googlecode.gchart.client.GChart;

public class SystemLoadAverageChart extends GChart {
    public SystemLoadAverageChart() {
        setChartSize(720, 200);
        setPadding("30px");
    }

    public void reset(int rrdPtr, JsArrayNumber times, JsArrayNumber systemLoadAverages) {
        clearCurves();
        addCurve();
        getCurve().setLegendLabel("System Load Average");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        int ptr = rrdPtr;
        for (int i = 0; i < times.length(); ++i) {
            getCurve().addPoint(i, systemLoadAverages.get(ptr));
            ptr = (ptr + 1) % times.length();
        }
        update();
    }
}