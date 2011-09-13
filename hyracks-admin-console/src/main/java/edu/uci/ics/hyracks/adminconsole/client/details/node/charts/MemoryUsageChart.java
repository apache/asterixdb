/*
 * Copyright 2009-2010 by The Regents of the University of California
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