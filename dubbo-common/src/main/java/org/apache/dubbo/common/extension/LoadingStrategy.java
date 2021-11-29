/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.extension;

import org.apache.dubbo.common.lang.Prioritized;

/**
 * Dubbo扩展的加载策略，有以下几种：
 * - DubboExternalLoadingStrategy，从META-INF/dubbo/external/位置加载扩展类，优先级：MAX_PRIORITY + 1
 * - DubboInternalLoadingStrategy，从META-INF/dubbo/internal/位置加载扩展类，优先级：MAX_PRIORITY
 * - DubboLoadingStrategy，从META-INF/dubbo/位置加载扩展类，优先级：NORMAL_PRIORITY
 * - ServicesLoadingStrategy，从META-INF/services/位置加载扩展类，优先级：MIN_PRIORITY
 */
public interface LoadingStrategy extends Prioritized {

    String directory();

    default boolean preferExtensionClassLoader() {
        return false;
    }

    default String[] excludedPackages() {
        return null;
    }

    /**
     * To restrict some class that should load from Dubbo's ClassLoader.
     * For example, we can restrict the class declaration in `org.apache.dubbo` package should
     * be loaded from Dubbo's ClassLoader and users cannot declare these classes.
     *
     * @return class packages should load
     * @since 3.0.4
     */
    default String[] onlyExtensionClassLoaderPackages() {
        return new String[]{};
    }

    /**
     * Indicates current {@link LoadingStrategy} supports overriding other lower prioritized instances or not.
     *
     * @return if supports, return <code>true</code>, or <code>false</code>
     * @since 2.7.7
     */
    default boolean overridden() {
        return false;
    }
}
