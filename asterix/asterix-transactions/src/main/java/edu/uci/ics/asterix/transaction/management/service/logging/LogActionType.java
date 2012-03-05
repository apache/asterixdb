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
package edu.uci.ics.asterix.transaction.management.service.logging;

public class LogActionType {

    public static final byte REDO = 0; // used for a log record that contains
    // just redo information.
    public static final byte REDO_UNDO = 1; // used for a log record that
    // contains both redo and undo
    // information.
    public static final byte UNDO = 2; // used for a log record that contains
    // just undo information.
    public static final byte NO_OP = 3; // used for a log record that does not
    // require any action.

}
