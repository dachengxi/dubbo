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
package org.apache.dubbo.remoting.transport.netty4;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.api.NettyEventLoopFactory;
import org.apache.dubbo.remoting.api.SslServerTlsHandler;
import org.apache.dubbo.remoting.transport.AbstractServer;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelHandlers;
import org.apache.dubbo.remoting.utils.UrlUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.IO_THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.KEEP_ALIVE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SSL_ENABLED_KEY;
import static org.apache.dubbo.remoting.Constants.EVENT_LOOP_BOSS_POOL_NAME;
import static org.apache.dubbo.remoting.Constants.EVENT_LOOP_WORKER_POOL_NAME;


/**
 * NettyServer.
 */
public class NettyServer extends AbstractServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    /**
     * the cache for alive worker channel.
     * <ip:port, dubbo channel>
     * 缓存客户端的通道
     */
    private Map<String, Channel> channels;

    /**
     * netty server bootstrap.
     *
     * Netty的服务启动类
     */
    private ServerBootstrap bootstrap;
    /**
     * the boss channel that receive connections and dispatch these to worker channel.
     */
    private io.netty.channel.Channel channel;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final int serverShutdownTimeoutMills;

    public NettyServer(URL url, ChannelHandler handler) throws RemotingException {
        // you can customize name and type of client thread pool by THREAD_NAME_KEY and THREADPOOL_KEY in CommonConstants.
        // the handler will be wrapped: MultiMessageHandler->HeartbeatHandler->handler
        // 父类中会进行Netty的相关实例化和绑定操作，开启Netty服务
        super(ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME), ChannelHandlers.wrap(handler, url));

        // read config before destroy
        serverShutdownTimeoutMills = ConfigurationUtils.getServerShutdownTimeout(getUrl().getOrDefaultModuleModel());
    }

    /**
     * Init and start netty server
     *
     * @throws Throwable
     *
     * 初始化和开始Netty服务
     */
    @Override
    protected void doOpen() throws Throwable {
        // 创建Netty的服务启动类对象
        bootstrap = new ServerBootstrap();

        // boss线程池
        bossGroup = NettyEventLoopFactory.eventLoopGroup(1, EVENT_LOOP_BOSS_POOL_NAME);

        // worker线程池
        workerGroup = NettyEventLoopFactory.eventLoopGroup(
                getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS),
            EVENT_LOOP_WORKER_POOL_NAME);

        // Netty服务端处理器
        final NettyServerHandler nettyServerHandler = new NettyServerHandler(getUrl(), this);
        channels = nettyServerHandler.getChannels();

        boolean keepalive = getUrl().getParameter(KEEP_ALIVE_KEY, Boolean.FALSE);

        // 设置Netty相关参数
        bootstrap.group(bossGroup, workerGroup)
                // 选择使用EpollServerSocketChannel还是NioServerSocketChannel
                .channel(NettyEventLoopFactory.serverSocketChannelClass())
                // 允许重复使用本地地址和端口
                .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                // 禁用Nagle算法，适用于小数据即时传输
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                // 如果在两小时内没有数据的通信时，TCP会自动发送一个活动探测的数据报文
                .childOption(ChannelOption.SO_KEEPALIVE, keepalive)
                // ByteBuf分配器，使用Netty默认的
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // FIXME: should we use getTimeout()?
                        int idleTimeout = UrlUtils.getIdleTimeout(getUrl());
                        NettyCodecAdapter adapter = new NettyCodecAdapter(getCodec(), getUrl(), NettyServer.this);
                        if (getUrl().getParameter(SSL_ENABLED_KEY, false)) {
                            ch.pipeline().addLast("negotiation", new SslServerTlsHandler(getUrl()));
                        }
                        ch.pipeline()
                                // 解码器
                                .addLast("decoder", adapter.getDecoder())
                                // 编码器
                                .addLast("encoder", adapter.getEncoder())
                                // 心跳管理
                                .addLast("server-idle-handler", new IdleStateHandler(0, 0, idleTimeout, MILLISECONDS))
                                // 业务自己的处理器
                                .addLast("handler", nettyServerHandler);
                    }
                });
        // bind
        // 启动服务
        ChannelFuture channelFuture = bootstrap.bind(getBindAddress());
        channelFuture.syncUninterruptibly();
        channel = channelFuture.channel();

    }

    @Override
    protected void doClose() throws Throwable {
        try {
            if (channel != null) {
                // unbind.
                channel.close();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            Collection<Channel> channels = getChannels();
            if (CollectionUtils.isNotEmpty(channels)) {
                for (Channel channel : channels) {
                    try {
                        channel.close();
                    } catch (Throwable e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (bootstrap != null) {
                long timeout = serverShutdownTimeoutMills;
                long quietPeriod = Math.min(2000L, timeout);
                Future<?> bossGroupShutdownFuture = bossGroup.shutdownGracefully(quietPeriod, timeout, MILLISECONDS);
                Future<?> workerGroupShutdownFuture = workerGroup.shutdownGracefully(quietPeriod, timeout, MILLISECONDS);
                bossGroupShutdownFuture.syncUninterruptibly();
                workerGroupShutdownFuture.syncUninterruptibly();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (channels != null) {
                channels.clear();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public Collection<Channel> getChannels() {
        Collection<Channel> chs = new ArrayList<>(this.channels.size());
        // pick channels from NettyServerHandler ( needless to check connectivity )
        chs.addAll(this.channels.values());
        return chs;
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return channels.get(NetUtils.toAddressString(remoteAddress));
    }

    @Override
    public boolean canHandleIdle() {
        return true;
    }

    @Override
    public boolean isBound() {
        return channel.isActive();
    }

}
