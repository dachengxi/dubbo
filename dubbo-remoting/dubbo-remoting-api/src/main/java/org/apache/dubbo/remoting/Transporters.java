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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.remoting.transport.ChannelHandlerAdapter;
import org.apache.dubbo.remoting.transport.ChannelHandlerDispatcher;

/**
 * Transporter facade. (API, Static, ThreadSafe)
 *
 * 传输层的门面类，传输层的统一入口
 */
public class Transporters {

    static {
        // check duplicate jar package
        Version.checkDuplicate(Transporters.class);
        Version.checkDuplicate(RemotingException.class);
    }

    private Transporters() {
    }

    /**
     * 服务端的绑定
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    public static RemotingServer bind(String url, ChannelHandler... handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * 服务端的绑定
     * @param url
     * @param handlers
     * @return
     * @throws RemotingException
     */
    public static RemotingServer bind(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers == null");
        }
        ChannelHandler handler;
        if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            // 有多个通道处理器的时候，封装成通道处理器分发器来统一管理
            handler = new ChannelHandlerDispatcher(handlers);
        }

        // 获取传输层扩展的实现，并进行绑定操作
        return getTransporter(url).bind(url, handler);
    }

    /**
     * 客户端连接到服务端
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    public static Client connect(String url, ChannelHandler... handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    /**
     * 客户端连接到服务端
     * @param url
     * @param handlers
     * @return
     * @throws RemotingException
     */
    public static Client connect(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        ChannelHandler handler;
        if (handlers == null || handlers.length == 0) {
            handler = new ChannelHandlerAdapter();
        } else if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            // 有多个通道处理器的时候，封装成通道处理器分发器来统一管理
            handler = new ChannelHandlerDispatcher(handlers);
        }
        // 获取传输层扩展的实现，并进行连接操作
        return getTransporter(url).connect(url, handler);
    }

    /**
     * 获取传输层扩展的实现
     * @param url
     * @return
     */
    public static Transporter getTransporter(URL url) {
        return url.getOrDefaultFrameworkModel().getExtensionLoader(Transporter.class).getAdaptiveExtension();
    }

}
