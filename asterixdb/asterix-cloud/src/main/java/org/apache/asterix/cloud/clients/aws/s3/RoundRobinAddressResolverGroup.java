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
package org.apache.asterix.cloud.clients.aws.s3;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.resolver.AbstractAddressResolver;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;

public final class RoundRobinAddressResolverGroup extends AddressResolverGroup<InetSocketAddress> {
    private static final Logger LOGGER = LogManager.getLogger();

    @Override
    protected AddressResolver<InetSocketAddress> newResolver(EventExecutor eventExecutor) throws Exception {
        return new AbstractAddressResolver<>(eventExecutor) {
            static final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

            @Override
            protected boolean doIsResolved(InetSocketAddress address) {
                return !address.isUnresolved();
            }

            @Override
            protected void doResolve(InetSocketAddress unresolved, Promise<InetSocketAddress> promise) {
                try {
                    InetAddress[] all = InetAddress.getAllByName(unresolved.getHostString());
                    int index = Math.floorMod(counter.getAndIncrement(), all.length);
                    InetSocketAddress resolved = new InetSocketAddress(all[index], unresolved.getPort());
                    LOGGER.debug("resolved {} ({}/{})", resolved, index + 1, all.length);
                    promise.setSuccess(resolved);
                } catch (Throwable t) {
                    promise.setFailure(t);
                }
            }

            @Override
            protected void doResolveAll(InetSocketAddress unresolved, Promise<List<InetSocketAddress>> promise) {
                try {
                    List<InetSocketAddress> list = Stream.of(InetAddress.getAllByName(unresolved.getHostString()))
                            .map(addr -> new InetSocketAddress(addr, unresolved.getPort())).toList();
                    promise.setSuccess(list);
                } catch (Throwable t) {
                    promise.setFailure(t);
                }
            }

        };
    }
}
