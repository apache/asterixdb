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
package org.apache.asterix.external.input.record.reader.awss3;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.input.record.reader.aws.AwsS3InputStreamFactory;
import org.junit.Assert;
import org.junit.Test;

import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3Test {

    @SuppressWarnings("unchecked")
    @Test
    public void testWorkloadDistribution() throws Exception {
        AwsS3InputStreamFactory factory = new AwsS3InputStreamFactory();

        List<S3Object> s3Objects = new ArrayList<>();
        final int partitionsCount = 3;

        // Create S3 objects, 9 objects, on 3 partitions, they should be 600 total size on each partition
        S3Object.Builder builder = S3Object.builder();
        s3Objects.add(builder.key("1.json").size(100L).build());
        s3Objects.add(builder.key("2.json").size(100L).build());
        s3Objects.add(builder.key("3.json").size(100L).build());
        s3Objects.add(builder.key("4.json").size(200L).build());
        s3Objects.add(builder.key("5.json").size(200L).build());
        s3Objects.add(builder.key("6.json").size(200L).build());
        s3Objects.add(builder.key("7.json").size(300L).build());
        s3Objects.add(builder.key("8.json").size(300L).build());
        s3Objects.add(builder.key("9.json").size(300L).build());

        // invoke the distributeWorkLoad method
        Method distributeWorkloadMethod =
                AwsS3InputStreamFactory.class.getDeclaredMethod("distributeWorkLoad", List.class, int.class);
        distributeWorkloadMethod.setAccessible(true);
        distributeWorkloadMethod.invoke(factory, s3Objects, partitionsCount);

        // get the partitionWorkLoadsBasedOnSize field and verify the result
        Field distributeWorkloadField =
                AwsS3InputStreamFactory.class.getSuperclass().getDeclaredField("partitionWorkLoadsBasedOnSize");
        distributeWorkloadField.setAccessible(true);
        List<AbstractExternalInputStreamFactory.PartitionWorkLoadBasedOnSize> workloads =
                (List<AbstractExternalInputStreamFactory.PartitionWorkLoadBasedOnSize>) distributeWorkloadField
                        .get(factory);

        for (AbstractExternalInputStreamFactory.PartitionWorkLoadBasedOnSize workload : workloads) {
            Assert.assertEquals(workload.getTotalSize(), 600);
        }
    }
}
