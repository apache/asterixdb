package edu.uci.ics.hyracks.adminconsole.client.details.node.charts;

import com.google.gwt.core.client.JsArrayNumber;
import com.googlecode.gchart.client.GChart;

public class MemoryUsageChart extends GChart {
    private String prefix;

    public MemoryUsageChart() {
        setChartSize(720, 200);
        setPadding("30px");
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void reset(int rrdPtr, JsArrayNumber times, JsArrayNumber initSizes, JsArrayNumber usedSizes,
            JsArrayNumber committedSizes, JsArrayNumber maxSizes) {
        clearCurves();
        addCurve();
        getCurve().setLegendLabel(prefix + " Initial Size");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        addCurve();
        getCurve().setLegendLabel(prefix + " Used Size");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        addCurve();
        getCurve().setLegendLabel(prefix + " Committed Size");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        addCurve();
        getCurve().setLegendLabel(prefix + " Maximum Size");
        getCurve().getSymbol().setWidth(0);
        getCurve().getSymbol().setHeight(0);
        getCurve().getSymbol().setSymbolType(SymbolType.LINE);
        int ptr = rrdPtr;
        for (int i = 0; i < times.length(); ++i) {
            getCurve(0).addPoint(i, initSizes.get(ptr));
            getCurve(1).addPoint(i, usedSizes.get(ptr));
            getCurve(2).addPoint(i, committedSizes.get(ptr));
            getCurve(3).addPoint(i, maxSizes.get(ptr));
            ptr = (ptr + 1) % times.length();
        }
        update();
    }
}