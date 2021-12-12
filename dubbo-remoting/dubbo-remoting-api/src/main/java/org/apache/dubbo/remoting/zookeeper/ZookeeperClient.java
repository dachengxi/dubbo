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
package org.apache.dubbo.remoting.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Zookeeper客户端的抽象
 */
public interface ZookeeperClient {

    /**
     * 创建节点
     * @param path
     * @param ephemeral
     */
    void create(String path, boolean ephemeral);

    /**
     * 删除节点
     * @param path
     */
    void delete(String path);

    /**
     * 获取子节点
     * @param path
     * @return
     */
    List<String> getChildren(String path);

    /**
     * 添加子节点监听器
     * @param path
     * @param listener
     * @return
     */
    List<String> addChildListener(String path, ChildListener listener);

    /**
     * @param path:    directory. All child of path will be listened.
     * @param listener
     *
     * 添加数据监听器
     */
    void addDataListener(String path, DataListener listener);

    /**
     * @param path:    directory. All child of path will be listened.
     * @param listener
     * @param executor another thread
     *
     * 添加数据监听器
     */
    void addDataListener(String path, DataListener listener, Executor executor);

    /**
     * 移除数据监听器
     * @param path
     * @param listener
     */
    void removeDataListener(String path, DataListener listener);

    /**
     * 移除子节点监听器
     * @param path
     * @param listener
     */
    void removeChildListener(String path, ChildListener listener);

    /**
     * 添加状态监听器
     * @param listener
     */
    void addStateListener(StateListener listener);

    /**
     * 移除状态监听器
     * @param listener
     */
    void removeStateListener(StateListener listener);

    /**
     * 是否已连接
     * @return
     */
    boolean isConnected();

    /**
     * 关闭
     */
    void close();

    /**
     * 获取URL
     * @return
     */
    URL getUrl();

    /**
     * 创建节点
     * @param path
     * @param content
     * @param ephemeral
     */
    void create(String path, String content, boolean ephemeral);

    /**
     * 创建或更新节点
     * @param path
     * @param content
     * @param ephemeral
     * @param ticket
     */
    void createOrUpdate(String path, String content, boolean ephemeral, int ticket);

    /**
     * 获取节点的内容
     * @param path
     * @return
     */
    String getContent(String path);

    /**
     * 获取内容和版本
     * @param path
     * @return
     */
    ConfigItem getConfigItem(String path);

    /**
     * 检查节点是否存在
     * @param path
     * @return
     */
    boolean checkExists(String path);

}
