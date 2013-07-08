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
package edu.uci.ics.hyracks.algebricks.core.algebra.scripting;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class StringStreamingScriptDescription implements IScriptDescription {

    private final String command;
    private final IPrinterFactory[] printerFactories;
    private final char fieldDelimiter;
    private final ITupleParserFactory parserFactory;
    private final List<Pair<LogicalVariable, Object>> varTypePairs;

    public StringStreamingScriptDescription(String command, IPrinterFactory[] printerFactories, char fieldDelimiter,
            ITupleParserFactory parserFactory, List<Pair<LogicalVariable, Object>> varTypePairs) {
        this.command = command;
        this.printerFactories = printerFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.parserFactory = parserFactory;
        this.varTypePairs = varTypePairs;
    }

    @Override
    public ScriptKind getKind() {
        return ScriptKind.STRING_STREAMING;
    }

    public String getCommand() {
        return command;
    }

    public IPrinterFactory[] getPrinterFactories() {
        return printerFactories;
    }

    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    public ITupleParserFactory getParserFactory() {
        return parserFactory;
    }

    @Override
    public List<Pair<LogicalVariable, Object>> getVarTypePairs() {
        return varTypePairs;
    }
}
