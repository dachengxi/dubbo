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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.resource.GlobalResourceInitializer;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.remoting.Constants.HEARTBEAT_CHECK_TICK;
import static org.apache.dubbo.remoting.Constants.LEAST_HEARTBEAT_DURATION;
import static org.apache.dubbo.remoting.Constants.TICKS_PER_WHEEL;
import static org.apache.dubbo.remoting.utils.UrlUtils.getHeartbeat;
import static org.apache.dubbo.remoting.utils.UrlUtils.getIdleTimeout;

/**
 * DefaultMessageClient
 *
 * 信息交换客户端的默认实现
 */
public class HeaderExchangeClient implements ExchangeClient {

    /**
     * 信息交换客户端持有的底层的客户端实现
     */
    private final Client client;

    /**
     * 信息交换客户端持有的信息交换通道
     */
    private final ExchangeChannel channel;

    public static GlobalResourceInitializer<HashedWheelTimer> IDLE_CHECK_TIMER = new GlobalResourceInitializer<>(() ->
        new HashedWheelTimer(new NamedThreadFactory("dubbo-client-idleCheck", true), 1,
            TimeUnit.SECONDS, TICKS_PER_WHEEL),
        timer -> timer.stop());

    /**
     * 重新连接的定时器
     */
    private Timeout reconnectTimer;

    /**
     * 心跳定时器
     */
    private Timeout heartBeatTimer;

    public HeaderExchangeClient(Client client, boolean startTimer) {
        Assert.notNull(client, "Client can't be null");
        this.client = client;
        this.channel = new HeaderExchangeChannel(client);

        if (startTimer) {
            URL url = client.getUrl();
            startReconnectTask(url);
            startHeartBeatTask(url);
        }
    }

    /**
     * 信息交换客户端发送请求
     * @param request
     * @return
     * @throws RemotingException
     */
    @Override
    public CompletableFuture<Object> request(Object request) throws RemotingException {
        return channel.request(request);
    }

    /**
     * 获取信息交换客户端的地址
     * @return
     */
    @Override
    public URL getUrl() {
        return channel.getUrl();
    }

    /**
     * 获取信息交换客户端的远程的地址
     * @return
     */
    @Override
    public InetSocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    /**
     * 信息交换客户端发送请求
     * @param request
     * @param timeout
     * @return
     * @throws RemotingException
     */
    @Override
    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        return channel.request(request, timeout);
    }

    /**
     * 信息交换客户端发送请求
     * @param request
     * @param executor
     * @return
     * @throws RemotingException
     */
    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        return channel.request(request, executor);
    }

    /**
     * 信息交换客户端发送请求
     * @param request
     * @param timeout
     * @param executor
     * @return
     * @throws RemotingException
     */
    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException {
        return channel.request(request, timeout, executor);
    }

    /**
     * 获取信息交换客户端的通道处理器
     * @return
     */
    @Override
    public ChannelHandler getChannelHandler() {
        return channel.getChannelHandler();
    }

    /**
     * 信息交换客户端是否已连接
     * @return
     */
    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    /**
     * 获取信息交换客户端本地地址
     * @return
     */
    @Override
    public InetSocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    /**
     * 获取信息交换客户端的信息交换处理器
     * @return
     */
    @Override
    public ExchangeHandler getExchangeHandler() {
        return channel.getExchangeHandler();
    }

    /**
     * 信息交换客户端发送消息
     * @param message
     * @throws RemotingException
     */
    @Override
    public void send(Object message) throws RemotingException {
        channel.send(message);
    }

    /**
     * 信息交换客户端发送消息
     * @param message
     * @param sent    already sent to socket?
     *
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        channel.send(message, sent);
    }

    /**
     * 信息交换客户端的通道是否已关闭
     * @return
     */
    @Override
    public boolean isClosed() {
        return channel.isClosed();
    }

    @Override
    public void close() {
        doClose();
        channel.close();
    }

    @Override
    public void close(int timeout) {
        // Mark the client into the closure process
        startClose();
        doClose();
        channel.close(timeout);
    }

    @Override
    public void startClose() {
        channel.startClose();
    }

    @Override
    public void reset(URL url) {
        client.reset(url);
        // FIXME, should cancel and restart timer tasks if parameters in the new URL are different?
    }

    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void reconnect() throws RemotingException {
        client.reconnect();
    }

    @Override
    public Object getAttribute(String key) {
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        return channel.hasAttribute(key);
    }

    private void startHeartBeatTask(URL url) {
        if (!client.canHandleIdle()) {
            AbstractTimerTask.ChannelProvider cp = () -> Collections.singletonList(HeaderExchangeClient.this);
            int heartbeat = getHeartbeat(url);
            long heartbeatTick = calculateLeastDuration(heartbeat);
            HeartbeatTimerTask heartBeatTimerTask = new HeartbeatTimerTask(cp, heartbeatTick, heartbeat);
            heartBeatTimer = IDLE_CHECK_TIMER.get().newTimeout(heartBeatTimerTask, heartbeatTick, TimeUnit.MILLISECONDS);
        }
    }

    private void startReconnectTask(URL url) {
        if (shouldReconnect(url)) {
            AbstractTimerTask.ChannelProvider cp = () -> Collections.singletonList(HeaderExchangeClient.this);
            int idleTimeout = getIdleTimeout(url);
            long heartbeatTimeoutTick = calculateLeastDuration(idleTimeout);
            ReconnectTimerTask reconnectTimerTask = new ReconnectTimerTask(cp, heartbeatTimeoutTick, idleTimeout);
            reconnectTimer = IDLE_CHECK_TIMER.get().newTimeout(reconnectTimerTask, heartbeatTimeoutTick, TimeUnit.MILLISECONDS);
        }
    }

    private void doClose() {
        if (heartBeatTimer != null) {
            heartBeatTimer.cancel();
            heartBeatTimer = null;
        }
        if (reconnectTimer != null) {
            reconnectTimer.cancel();
            reconnectTimer = null;
        }
    }

    /**
     * Each interval cannot be less than 1000ms.
     */
    private long calculateLeastDuration(int time) {
        if (time / HEARTBEAT_CHECK_TICK <= 0) {
            return LEAST_HEARTBEAT_DURATION;
        } else {
            return time / HEARTBEAT_CHECK_TICK;
        }
    }

    private boolean shouldReconnect(URL url) {
        return url.getParameter(Constants.RECONNECT_KEY, true);
    }

    @Override
    public String toString() {
        return "HeaderExchangeClient [channel=" + channel + "]";
    }
}
