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
package org.apache.dubbo.registry.client.event.listener;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.event.ConditionalEventListener;
import org.apache.dubbo.event.EventListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.MetadataInfo.ServiceInfo;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.RegistryClusterIdentifier;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.registry.client.metadata.store.RemoteMetadataServiceImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.dubbo.common.constants.CommonConstants.REGISTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;

/**
 * The Service Discovery Changed {@link EventListener Event Listener}
 *
 * @see ServiceInstancesChangedEvent
 * @since 2.7.5
 */
public class ServiceInstancesChangedListener implements ConditionalEventListener<ServiceInstancesChangedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(ServiceInstancesChangedListener.class);

    private final Set<String> serviceNames;
    private final ServiceDiscovery serviceDiscovery;
    private URL url;
    private Map<String, NotifyListener> listeners;

    private Map<String, List<ServiceInstance>> allInstances;

    private Map<String, List<URL>> serviceUrls;

    private Map<String, MetadataInfo> revisionToMetadata;

    public ServiceInstancesChangedListener(Set<String> serviceNames, ServiceDiscovery serviceDiscovery) {
        this.serviceNames = serviceNames;
        this.serviceDiscovery = serviceDiscovery;
        this.listeners = new HashMap<>();
        this.allInstances = new HashMap<>();
        this.serviceUrls = new HashMap<>();
        this.revisionToMetadata = new HashMap<>();
    }

    /**
     * On {@link ServiceInstancesChangedEvent the service instances change event}
     *
     * @param event {@link ServiceInstancesChangedEvent}
     */
    public synchronized void onEvent(ServiceInstancesChangedEvent event) {
        String appName = event.getServiceName();
        allInstances.put(appName, event.getServiceInstances());

        for (Map.Entry<String, List<ServiceInstance>> entry : allInstances.entrySet()) {
            List<ServiceInstance> instances = entry.getValue();
            for (ServiceInstance instance : instances) {
                String revision = getExportedServicesRevision(instance);
                Map<String, List<ServiceInstance>> revisionToInstances = new HashMap<>();
                List<ServiceInstance> subInstances = revisionToInstances.computeIfAbsent(revision, r -> new LinkedList<>());
                subInstances.add(instance);

                MetadataInfo metadata = revisionToMetadata.get(revision);
                if (metadata == null) {
                    metadata = getMetadataInfo(instance);
                    if (metadata != null) {
                        revisionToMetadata.put(revision, getMetadataInfo(instance));
                    } else {

                    }
                }

                Map<String, Set<String>> localServiceToRevisions = new HashMap<>();
                if (metadata != null) {
                    parseMetadata(revision, metadata, localServiceToRevisions);
                    ((DefaultServiceInstance) instance).setServiceMetadata(metadata);
                }
//                else {
//                    logger.error("Failed to load service metadata for instance " + instance);
//                    Set<String> set = localServiceToRevisions.computeIfAbsent(url.getServiceKey(), k -> new TreeSet<>());
//                    set.add(revision);
//                }

                Map<Set<String>, List<URL>> revisionsToUrls = new HashMap();
                localServiceToRevisions.forEach((serviceKey, revisions) -> {
                    List<URL> urls = revisionsToUrls.get(revisions);
                    if (urls != null) {
                        serviceUrls.put(serviceKey, urls);
                    } else {
                        urls = new ArrayList<>();
                        for (String r : revisions) {
                            for (ServiceInstance i : revisionToInstances.get(r)) {
                                urls.add(i.toURL());
                            }
                        }
                        revisionsToUrls.put(revisions, urls);
                        serviceUrls.put(serviceKey, urls);
                    }
                });
            }
        }

        this.notifyAddressChanged();
    }

    private Map<String, Set<String>> parseMetadata(String revision, MetadataInfo metadata, Map<String, Set<String>> localServiceToRevisions) {
        Map<String, ServiceInfo> serviceInfos = metadata.getServices();
        for (Map.Entry<String, ServiceInfo> entry : serviceInfos.entrySet()) {
            String serviceKey = entry.getValue().getServiceKey();
            Set<String> set = localServiceToRevisions.computeIfAbsent(serviceKey, k -> new TreeSet<>());
            set.add(revision);
        }

        return localServiceToRevisions;
    }

    private MetadataInfo getMetadataInfo(ServiceInstance instance) {
        String metadataType = ServiceInstanceMetadataUtils.getMetadataStorageType(instance);
        instance.getExtendParams().putIfAbsent(REGISTER_KEY, RegistryClusterIdentifier.getExtension().consumerKey(url));
        MetadataInfo metadataInfo;
        try {
            if (REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                RemoteMetadataServiceImpl remoteMetadataService = MetadataUtils.getRemoteMetadataService();
                metadataInfo = remoteMetadataService.getMetadata(instance);
            } else {
                MetadataService metadataServiceProxy = MetadataUtils.getMetadataServiceProxy(instance);
                metadataInfo = metadataServiceProxy.getMetadataInfo(ServiceInstanceMetadataUtils.getExportedServicesRevision(instance));
            }
        } catch (Exception e) {
            // TODO, load metadata backup
            metadataInfo = null;
        }
        return metadataInfo;
    }

    private void notifyAddressChanged() {
        listeners.forEach((key, notifyListener) -> {
            //FIXME, group wildcard match
            notifyListener.notify(serviceUrls.get(key));
        });
    }

    public void addListener(String serviceKey, NotifyListener listener) {
        this.listeners.put(serviceKey, listener);
    }

    public void removeListener(String serviceKey) {
        this.listeners.remove(serviceKey);
    }

    public List<URL> getUrls(String serviceKey) {
        return serviceUrls.get(serviceKey);
    }

    /**
     * Get the correlative service name
     *
     * @return the correlative service name
     */
    public final Set<String> getServiceNames() {
        return serviceNames;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    public URL getUrl() {
        return url;
    }

    /**
     * @param event {@link ServiceInstancesChangedEvent event}
     * @return If service name matches, return <code>true</code>, or <code>false</code>
     */
    public final boolean accept(ServiceInstancesChangedEvent event) {
        return serviceNames.contains(event.getServiceName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServiceInstancesChangedListener)) return false;
        ServiceInstancesChangedListener that = (ServiceInstancesChangedListener) o;
        return Objects.equals(getServiceNames(), that.getServiceNames());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), getServiceNames());
    }
}
