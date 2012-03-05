/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.common;

/**
 * This class provides constants for message types in the AQLJ protocol.
 * 
 * @author zheilbron
 */
public abstract class AQLJProtocol {
    public static final char STARTUP_MESSAGE = 'S';
    public static final char EXECUTE_MESSAGE = 'X';
    public static final char READY_MESSAGE = 'R';
    public static final char ERROR_MESSAGE = 'E';
    public static final char EXECUTE_COMPLETE_MESSAGE = 'C';
    public static final char DATA_MESSAGE = 'D';
    public static final char GET_RESULTS_MESSAGE = 'G';
}
