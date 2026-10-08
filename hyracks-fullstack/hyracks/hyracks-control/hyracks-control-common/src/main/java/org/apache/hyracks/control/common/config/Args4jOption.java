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

import java.lang.annotation.Annotation;

import org.apache.hyracks.api.config.IOption;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.ExplicitBooleanOptionHandler;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.StringOptionHandler;

@SuppressWarnings("ClassExplicitlyAnnotation")
class Args4jOption implements Option {
    private final IOption option;
    private final ConfigManager configManager;
    private final Class targetType;

    Args4jOption(IOption option, ConfigManager configManager, Class targetType) {
        this.option = option;
        this.targetType = targetType;
        this.configManager = configManager;
    }

    @Override
    public String name() {
        return option.cmdline();
    }

    @Override
    public String[] aliases() {
        return new String[0];
    }

    @Override
    public String usage() {
        return configManager.getUsage(option);
    }

    @Override
    public String metaVar() {
        return "";
    }

    @Override
    public boolean required() {
        return false;
    }

    @Override
    public boolean help() {
        return false;
    }

    @Override
    public boolean hidden() {
        return option.hidden();
    }

    @Override
    public Class<? extends OptionHandler> handler() {
        if (targetType.equals(Boolean.class)) {
            return ExplicitBooleanOptionHandler.class;
        } else if (targetType.equals(Integer.class)) {
            return IntOptionHandler.class;
        } else {
            return StringOptionHandler.class;
        }
    }

    @Override
    public String[] depends() {
        return new String[0];
    }

    @Override
    public String[] forbids() {
        return new String[0];
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return Option.class;
    }
}
