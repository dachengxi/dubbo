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
package org.apache.dubbo.bootstrap;

import org.apache.dubbo.bootstrap.rest.UserService;
import org.apache.dubbo.bootstrap.rest.UserServiceImpl;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.builders.RegistryBuilder;

/**
 * Dubbo Provider Bootstrap
 *
 * @since 2.7.4
 */
public class DubboServiceProviderBootstrap {

    public static void main(String[] args) {
        ProtocolConfig restProtocol = new ProtocolConfig();
        restProtocol.setName("rest");
        restProtocol.setId("rest");
        restProtocol.setPort(-1);

        new DubboBootstrap()
                .application("dubbo-provider-demo")
                // Zookeeper in service registry type
//                .registry("zookeeper", builder -> builder.address("zookeeper://127.0.0.1:2181?registry-type=service"))
                // Nacos
//                .registry("zookeeper", builder -> builder.address("nacos://127.0.0.1:8848?registry-type=service"))
                .registry(RegistryBuilder.newBuilder().address("consul://127.0.0.1:8500?registry-type=service").build())
                .protocol(builder -> builder.port(-1).name("dubbo"))
                .service(builder -> builder.id("echo").interfaceClass(EchoService.class).ref(new EchoServiceImpl()))
                .service(builder -> builder.id("user").interfaceClass(UserService.class).ref(new UserServiceImpl()).addProtocol(restProtocol))
                .start()
                .await();
    }

    private static void testSCCallDubbo() {

    }

    private static void testDubboCallSC() {

    }

    private static void testDubboTansormation() {

    }
}
