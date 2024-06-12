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
package org.apache.asterix.external.writer.printer.parquet;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class ParquetSchemaBuilderUtils {

    public static Types.BaseGroupBuilder<?, ?> getGroupChild(Types.Builder parent) {
        if (parent instanceof Types.BaseGroupBuilder) {
            return ((Types.BaseGroupBuilder<?, ?>) parent).optionalGroup();
        } else if (parent instanceof Types.BaseListBuilder) {
            return ((Types.BaseListBuilder<?, ?>) parent).optionalGroupElement();
        } else {
            return null;
        }
    }

    public static Types.BaseListBuilder<?, ?> getListChild(Types.Builder parent) {
        if (parent instanceof Types.BaseGroupBuilder) {
            return ((Types.BaseGroupBuilder<?, ?>) parent).optionalList();
        } else if (parent instanceof Types.BaseListBuilder) {
            return ((Types.BaseListBuilder<?, ?>) parent).optionalListElement();
        } else {
            return null;
        }
    }

    public static Types.Builder<?, ?> getPrimitiveChild(Types.Builder parent, PrimitiveType.PrimitiveTypeName type,
            LogicalTypeAnnotation annotation) {
        if (parent instanceof Types.BaseGroupBuilder) {
            return ((Types.BaseGroupBuilder<?, ?>) parent).optional(type).as(annotation);
        } else if (parent instanceof Types.BaseListBuilder) {
            return ((Types.BaseListBuilder<?, ?>) parent).optionalElement(type).as(annotation);
        } else {
            return null;
        }
    }
}
