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
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporters;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchanger;
import org.apache.dubbo.remoting.transport.DecodeHandler;

/**
 * DefaultMessenger
 *
 * 信息交换层Exchanger的默认实现
 */
public class HeaderExchanger implements Exchanger {

    public static final String NAME = "header";

    /**
     * 客户端的链接操作，客户端连接到服务端
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    @Override
    public ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        /*
            ChannelHandler exchangeHandler = new HeaderExchangeHandler(handler);
            ChannelHandler decodeHandler = new DecodeHandler(exchangeHandler);
            Client client = Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler)));
            HeaderExchangeClient exchangeClient = new HeaderExchangeClient(client, true);
            return exchangeClient;
         */
        return new HeaderExchangeClient(Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))), true);
    }

    /**
     * 服务端的绑定操作，服务端绑定端口、暴露服务
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    @Override
    public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        /*
            ChannelHandler exchangeHandler = new HeaderExchangeHandler(handler);
            ChannelHandler decodeHandler = new DecodeHandler(exchangeHandler);
            RemotingServer server = Transporters.bind(url, decodeHandler);
            HeaderExchangeServer exchangeServer = new HeaderExchangeServer(server);
            return exchangeServer;
         */
        return new HeaderExchangeServer(Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
    }

}
