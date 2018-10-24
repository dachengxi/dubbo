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
package org.apache.dubbo.metadata.integration;

import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.metadata.metadata.ServiceDescriptor;
import org.apache.dubbo.metadata.metadata.builder.ServiceDescriptorBuilder;
import org.apache.dubbo.metadata.store.MetadataReport;
import org.apache.dubbo.metadata.store.MetadataReportFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.dubbo.common.Constants.SERVICE_DESCIPTOR_KEY;

/**
 * @since 2.7.0
 */
public class MetadataReportService {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private static final int ONE_DAY_IN_MIll = 60 * 24 * 60 * 1000;
    private static final int FOUR_HOURS_IN_MIll = 60 * 4 * 60 * 1000;

    private static MetadataReportService metadataReportService;
    private static Object lock = new Object();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(0, new NamedThreadFactory("DubboMetadataReportTimer", true));
    private MetadataReportFactory metadataReportFactory = ExtensionLoader.getExtensionLoader(MetadataReportFactory.class).getAdaptiveExtension();
    final Set<URL> providerURLs = new ConcurrentHashSet<>();
    final Set<URL> consumerURLs = new ConcurrentHashSet<URL>();
    MetadataReport metadataReport;
    URL metadataReportUrl;

    MetadataReportService(URL metadataReportURL) {
        if (Constants.SERVICE_STORE_KEY.equals(metadataReportURL.getProtocol())) {
            String protocol = metadataReportURL.getParameter(Constants.SERVICE_STORE_KEY, Constants.DEFAULT_DIRECTORY);
            metadataReportURL = metadataReportURL.setProtocol(protocol).removeParameter(Constants.SERVICE_STORE_KEY);
        }
        this.metadataReportUrl = metadataReportURL;
        metadataReport = metadataReportFactory.getMetadataReport(this.metadataReportUrl);
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                publishAll();
            }
        }, calculateStartTime(), ONE_DAY_IN_MIll, TimeUnit.MILLISECONDS);
    }


    public static MetadataReportService instance(Supplier<URL> loadServiceStoreUrl) {
        if (metadataReportService == null) {
            synchronized (lock) {
                if (metadataReportService == null) {
                    URL serviceStoreURL = loadServiceStoreUrl.get();
                    if (serviceStoreURL == null) {
                        return null;
                    }
                    metadataReportService = new MetadataReportService(serviceStoreURL);
                }
            }
        }
        return metadataReportService;
    }

    public void publishProvider(URL providerUrl) throws RpcException {
        //first add into the list
        providerURLs.add(providerUrl);
        try {
            String interfaceName = providerUrl.getParameter(Constants.INTERFACE_KEY);
            if (StringUtils.isNotEmpty(interfaceName)) {
                Class interfaceClass = Class.forName(interfaceName);
                ServiceDescriptor serviceDescriptor = ServiceDescriptorBuilder.build(interfaceClass);
                providerUrl = providerUrl.addParameter(SERVICE_DESCIPTOR_KEY, JSON.toJSONString(serviceDescriptor));
            }
        } catch (ClassNotFoundException e) {
            //ignore error
            logger.error("Servicestore getServiceDescriptor error. providerUrl: " + providerUrl.toFullString(), e);
        }
        metadataReport.put(providerUrl);
    }

    public void publishConsumer(URL consumerURL) throws RpcException {
        consumerURLs.add(consumerURL);
        metadataReport.put(consumerURL);
    }

    void publishAll() {
        for (URL url : providerURLs) {
            publishProvider(url);
        }
        for (URL url : consumerURLs) {
            publishConsumer(url);
        }
    }

    /**
     * between 2:00 am to 6:00 am, the time is random.
     * @return
     */
    long calculateStartTime() {
        Date now = new Date();
        long nowMill = now.getTime();
        long today0 = DateUtils.truncate(now, Calendar.DAY_OF_MONTH).getTime();
        long subtract = today0 + ONE_DAY_IN_MIll - nowMill;
        Random r = new Random();
        return subtract + (FOUR_HOURS_IN_MIll / 2) + r.nextInt(FOUR_HOURS_IN_MIll);
    }

}
