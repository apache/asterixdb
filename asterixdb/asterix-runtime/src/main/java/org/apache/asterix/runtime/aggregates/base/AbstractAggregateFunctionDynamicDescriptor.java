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
package org.apache.asterix.runtime.aggregates.base;

import java.util.function.Supplier;

import org.apache.asterix.common.functions.FunctionDescriptorTag;
import org.apache.asterix.om.functions.AbstractFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;

public abstract class AbstractAggregateFunctionDynamicDescriptor extends AbstractFunctionDescriptor {
    private static final long serialVersionUID = 1L;

    public static IFunctionDescriptorFactory createFactory(Supplier<IFunctionDescriptor> descriptorSupplier) {
        return DescriptorFactoryUtil.createFactory(descriptorSupplier, FunctionTypeInferers.SET_ARGUMENT_TYPE);
    }

    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return FunctionDescriptorTag.AGGREGATE;
    }

}
