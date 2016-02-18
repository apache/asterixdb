/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.experiment.action.derived;

import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.asterix.experiment.action.base.IAction;

public class TimedAction extends AbstractAction {

    private final Logger LOGGER = Logger.getLogger(TimedAction.class.getName());

    private final IAction action;
    private final OutputStream os;

    public TimedAction(IAction action) {
        this.action = action;
        os = null;
    }

    public TimedAction(IAction action, OutputStream os) {
        this.action = action;
        this.os = os;
    }

    @Override
    protected void doPerform() throws Exception {
        long start = System.currentTimeMillis();
        action.perform();
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.SEVERE)) {
            if (os == null) {
                System.out.println("Elapsed time = " + (end - start) + " for action " + action);
                System.out.flush();
            } else {
                os.write(("Elapsed time = " + (end - start) + " for action " + action).getBytes());
                os.flush();
            }
        }
    }
}
