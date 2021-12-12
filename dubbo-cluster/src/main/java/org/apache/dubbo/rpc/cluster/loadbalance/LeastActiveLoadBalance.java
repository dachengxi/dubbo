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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 *
 * 最少活跃调用数的负载均衡策略
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    /**
     * 从活跃请求数最少的Invoker集合中挑选Invoker
     * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        // Invoker的数量
        int length = invokers.size();

        // The least active value of all invokers
        // 记录最少的活跃请求数
        int leastActive = -1;

        // The number of invokers having the same least active value (leastActive)
        // 记录活跃请求数最小的Invoker集合的个数
        int leastCount = 0;

        // The index of invokers having the same least active value (leastActive)
        // 记录活跃请求数最小的Invoker在Invoker列表中的索引
        int[] leastIndexes = new int[length];

        // the weight of every invokers
        // 活跃请求数最小的Invoker集合中每个Invoker的权重
        int[] weights = new int[length];

        // The sum of the warmup weights of all the least active invokers
        // 活跃请求数最小的Invoker集合中所有Invoker总权重
        int totalWeight = 0;

        // The weight of the first least active invoker
        // 活跃请求数最小的Invoker集合中第一个Invoker的权重
        int firstWeight = 0;

        // Every least active invoker has the same weight value?
        // 活跃请求数最小的Invoker集合中是不是所有的Invoker集合的权重都一样
        boolean sameWeight = true;


        // Filter out all the least active invokers
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);

            // Get the active number of the invoker
            // Invoker的活跃请求数
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();

            // Get the weight of the invoker's configuration. The default value is 100.
            // Invoker的权重
            int afterWarmup = getWeight(invoker, invocation);
            // save for later use
            weights[i] = afterWarmup;
            // If it is the first invoker or the active number of the invoker is less than the current least active number
            // 第一个Invoker或者是当前的Invoker的请求数比之前的Invoker的请求数还要小
            if (leastActive == -1 || active < leastActive) {
                // Reset the active number of the current invoker to the least active number
                // 记录最小的活跃请求数
                leastActive = active;

                // Reset the number of least active invokers
                // 重新记录活跃请求数最小的Invoker的集合个数
                leastCount = 1;

                // Put the first least active invoker first in leastIndexes
                // 重新记录Invoker的索引
                leastIndexes[0] = i;

                // Reset totalWeight
                // 总权重
                totalWeight = afterWarmup;

                // Record the weight the first least active invoker
                // 最小活跃数集合中第一个Invoker
                firstWeight = afterWarmup;
                // Each invoke has the same weight (only one invoker here)
                sameWeight = true;
                // If current invoker's active value equals with leaseActive, then accumulating.
            }
            // 当前活跃数是最小活跃数
            else if (active == leastActive) {
                // Record the index of the least active invoker in leastIndexes order
                // 最小活跃数加1，将当前Invoker的下标记录下来
                leastIndexes[leastCount++] = i;

                // Accumulate the total weight of the least active invoker
                // 总权重
                totalWeight += afterWarmup;
                // If every invoker has the same weight?
                // 判断是不是所有Invoker权重都一样
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // Choose an invoker from all the least active invokers
        // 最少活跃数的Invoker列表中只包含一个Invoker，直接返回这个Invoker
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexes[0]);
        }

        // 最少活跃数的Invoker列表中的Invoker的权重不一样，需要使用加权随机选择一个Invoker
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on 
            // totalWeight.
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        // 所有的Invoker权重一样，随机选择一个
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
