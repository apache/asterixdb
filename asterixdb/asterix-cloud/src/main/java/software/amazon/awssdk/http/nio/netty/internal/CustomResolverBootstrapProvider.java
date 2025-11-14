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

// original copyright appears below
/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.http.nio.netty.internal;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.resolver.AddressResolverGroup;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;

/**
 * The primary purpose of this Bootstrap provider is to ensure that all Bootstraps created by it are 'unresolved'
 * InetSocketAddress. This is to prevent Netty from caching the resolved address of a host and then re-using it in
 * subsequent connection attempts, and instead deferring to the JVM to handle address resolution and caching.
 */
public class CustomResolverBootstrapProvider extends BootstrapProvider {
    private final AddressResolverGroup<InetSocketAddress> customResolverGroup;

    CustomResolverBootstrapProvider(SdkEventLoopGroup sdkEventLoopGroup, NettyConfiguration nettyConfiguration,
            SdkChannelOptions sdkChannelOptions, AddressResolverGroup<InetSocketAddress> customResolverGroup) {
        super(sdkEventLoopGroup, nettyConfiguration, sdkChannelOptions);
        this.customResolverGroup = customResolverGroup;
    }

    public static void bindTo(SdkAsyncHttpClient nettyClient,
            AddressResolverGroup<InetSocketAddress> customResolverGroup) throws Exception {

        Field poolsField = getAccessibleField(nettyClient.getClass(), "pools");
        Field bootstrapProviderField = getAccessibleField(AwaitCloseChannelPoolMap.class, "bootstrapProvider");
        Object poolsInstance = poolsField.get(nettyClient);
        Object bootstrapProviderInstance = bootstrapProviderField.get(poolsInstance);
        SdkEventLoopGroup sdkEventLoopGroup =
                (SdkEventLoopGroup) getAccessibleField(BootstrapProvider.class, "sdkEventLoopGroup")
                        .get(bootstrapProviderInstance);
        NettyConfiguration nettyConfiguration =
                (NettyConfiguration) getAccessibleField(BootstrapProvider.class, "nettyConfiguration")
                        .get(bootstrapProviderInstance);
        SdkChannelOptions sdkChannelOptions =
                (SdkChannelOptions) getAccessibleField(BootstrapProvider.class, "sdkChannelOptions")
                        .get(bootstrapProviderInstance);
        bootstrapProviderField.set(poolsInstance, new CustomResolverBootstrapProvider(sdkEventLoopGroup,
                nettyConfiguration, sdkChannelOptions, customResolverGroup));
    }

    private static Field getAccessibleField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    /**
     * Creates a Bootstrap for a specific host and port with an unresolved InetSocketAddress as the remoteAddress.
     *
     * @param host                      The unresolved remote hostname
     * @param port                      The remote port
     * @param useNonBlockingDnsResolver If true, uses the default non-blocking DNS resolver from Netty. Otherwise, the default
     *                                  JDK blocking DNS resolver will be used.
     * @return A newly created Bootstrap using the configuration this provider was initialized with, and having an unresolved
     * remote address.
     */
    public Bootstrap createBootstrap(String host, int port, Boolean useNonBlockingDnsResolver) {
        Bootstrap bootstrap = super.createBootstrap(host, port, useNonBlockingDnsResolver);
        bootstrap.resolver(customResolverGroup);
        return bootstrap;
    }

}
