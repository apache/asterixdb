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
package org.apache.hyracks.control.common.config;

import java.lang.reflect.AnnotatedElement;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.hyracks.api.config.IOption;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.spi.FieldSetter;
import org.kohsuke.args4j.spi.Setter;

class Args4jSetter implements Setter {
    private final IOption option;
    private BiConsumer<IOption, Object> consumer;
    private final boolean multiValued;
    private final Class type;

    Args4jSetter(IOption option, BiConsumer<IOption, Object> consumer, boolean multiValued) {
        this.option = option;
        this.consumer = consumer;
        this.multiValued = multiValued;
        this.type = option.type().targetType();
    }

    Args4jSetter(Consumer<Object> consumer, boolean multiValued, Class type) {
        this.option = null;
        this.consumer = (o, value) -> consumer.accept(value);
        this.multiValued = multiValued;
        this.type = type;
    }

    @Override
    public void addValue(Object value) throws CmdLineException {
        consumer.accept(option, value);
    }

    @Override
    public Class getType() {
        return type;
    }

    @Override
    public boolean isMultiValued() {
        return multiValued;
    }

    @Override
    public FieldSetter asFieldSetter() {
        return null;
    }

    @Override
    public AnnotatedElement asAnnotatedElement() {
        return null;
    }
}
