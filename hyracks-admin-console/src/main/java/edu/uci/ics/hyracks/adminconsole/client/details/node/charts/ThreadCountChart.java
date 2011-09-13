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