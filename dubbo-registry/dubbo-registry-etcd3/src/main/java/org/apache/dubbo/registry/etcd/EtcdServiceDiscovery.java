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
package org.apache.dubbo.registry.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.event.EventListener;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.remoting.etcd.ChildListener;
import org.apache.dubbo.remoting.etcd.EtcdClient;
import org.apache.dubbo.remoting.etcd.EtcdTransporter;
import org.apache.dubbo.remoting.etcd.StateListener;
import org.apache.dubbo.remoting.etcd.option.OptionUtil;
import org.apache.dubbo.rpc.RpcException;

import com.google.gson.Gson;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;

/**
 * 2019-07-08
 */
public class EtcdServiceDiscovery implements ServiceDiscovery, EventListener<ServiceInstancesChangedEvent> {

    private final static Logger logger = LoggerFactory.getLogger(EtcdServiceDiscovery.class);

    private final String root = "/services";

    private final Set<String> services = new ConcurrentHashSet<>();
    private final Map<String, ChildListener> childListenerMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> etcdListeners = new ConcurrentHashMap<>();
    private final EtcdClient etcdClient;
    private final EventDispatcher dispatcher;

    public EtcdServiceDiscovery(URL url, EtcdTransporter etcdTransporter) {
        if (url.isAnyHost()) {
            throw new IllegalStateException("Service discovery address is invalid, actual: '" + url.getHost() + "'");
        }
        etcdClient = etcdTransporter.connect(url);

        etcdClient.addStateListener(state -> {
            if (state == StateListener.CONNECTED) {
                try {
//                    recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });

        this.dispatcher = EventDispatcher.getDefaultExtension();
        this.dispatcher.addEventListener(this);
    }

    @Override
    public void onEvent(ServiceInstancesChangedEvent event) {
        registerServiceWatcher(event.getServiceName());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void register(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            String path = toPath(serviceInstance);
            etcdClient.put(path, new Gson().toJson(serviceInstance));
            services.add(serviceInstance.getServiceName());
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + serviceInstance + " to etcd " + etcdClient.getUrl()
                    + ", cause: " + (OptionUtil.isProtocolError(e)
                    ? "etcd3 registry may not be supported yet or etcd3 registry is not available."
                    : e.getMessage()), e);
        }
    }

    String toPath(ServiceInstance serviceInstance) {
        return root + File.separator + serviceInstance.getServiceName() + File.separator + serviceInstance.getHost()
                + ":" + serviceInstance.getPort();
    }

    @Override
    public void update(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            String path = toPath(serviceInstance);
            etcdClient.put(path, new Gson().toJson(serviceInstance));
            services.add(serviceInstance.getServiceName());
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + serviceInstance + " to etcd " + etcdClient.getUrl()
                    + ", cause: " + (OptionUtil.isProtocolError(e)
                    ? "etcd3 registry may not be supported yet or etcd3 registry is not available."
                    : e.getMessage()), e);
        }
    }

    @Override
    public void unregister(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            String path = toPath(serviceInstance);
            etcdClient.delete(path);
            services.remove(serviceInstance.getServiceName());
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + serviceInstance + " to etcd " + etcdClient.getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public Set<String> getServices() {
        return Collections.unmodifiableSet(services);
    }

    @Override
    public void addServiceInstancesChangedListener(String serviceName, ServiceInstancesChangedListener listener) throws NullPointerException, IllegalArgumentException {
        registerServiceWatcher(serviceName);
        dispatcher.addEventListener(listener);
    }

    protected void registerServiceWatcher(String serviceName) {
        String path = root + File.separator + serviceName;
        /*
         *  if we have no category watcher listener,
         *  we find out the current listener or create one for the current category, put or get only once.
         */
        ChildListener childListener =
                Optional.ofNullable(childListenerMap.get(serviceName))
                        .orElseGet(() -> {
                            ChildListener watchListener, prev;
                            prev = childListenerMap.putIfAbsent(serviceName, watchListener = (parentPath, currentChildren) ->
                                    dispatcher.dispatch(new ServiceInstancesChangedEvent(serviceName, getInstances(serviceName))));
                            return prev != null ? prev : watchListener;
                        });

        etcdClient.create(path);
        /*
         * at the first time, we want to pull already category and then watch their direct children,
         *  eg: /dubbo/interface/providers, /dubbo/interface/consumers and so on.
         */
        List<String> children = etcdClient.addChildListener(path, childListener);
    }
}
