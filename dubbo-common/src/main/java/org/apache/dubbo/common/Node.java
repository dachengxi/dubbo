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
package org.apache.dubbo.common;

/**
 * Node. (API/SPI, Prototype, ThreadSafe)
 *
 * 服务节点抽象，不管是一个服务提供者还是一个消费者，都可以认为是一个节点
 */
public interface Node {

    /**
     * get url.
     *
     * @return url.
     *
     * 获取URL
     */
    URL getUrl();

    /**
     * is available.
     *
     * @return available.
     *
     * 节点是否可用
     */
    boolean isAvailable();

    /**
     * destroy.
     *
     * 销毁
     */
    void destroy();

}
