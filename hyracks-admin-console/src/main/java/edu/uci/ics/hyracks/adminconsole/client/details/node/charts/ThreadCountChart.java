package edu.uci.ics.hyracks.adminconsole.client.details.node.charts;

import com.google.gwt.core.client.JsArrayInteger;
import com.google.gwt.core.client.JsArrayNumber;
import com.googlecode.gchart.client.GChart;

public class ThreadCountChart extends GChart {
    public ThreadCountChart() {
        setChartSize(720, 200);
        setPadding("30px");
    }

    public void reset(int rrdPtr, JsArrayNumber times, JsArrayInteger threadCounts, JsArrayInteger peakThreadCounts) {
        clearCurves();
        addCurve();
        getCurve().setLegendLabel("Thread Count");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        addCurve();
        getCurve().setLegendLabel("Peak Thread Count");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        int ptr = rrdPtr;
        for (int i = 0; i < times.length(); ++i) {
            getCurve(0).addPoint(i, threadCounts.get(ptr));
            getCurve(1).addPoint(i, peakThreadCounts.get(ptr));
            ptr = (ptr + 1) % times.length();
        }
        update();
    }
}