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
package com.alipay.remoting;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.alipay.remoting.config.ConfigManager;
import com.alipay.remoting.config.Configs;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.util.FutureTaskUtil;
import com.alipay.remoting.util.RemotingUtil;
import com.alipay.remoting.util.RunStateRecordedFutureTask;

/**
 * An implemented strategy to monitor connections:
 *   <lu>
 *       <li>each time scheduled, filter connections with {@link Configs#CONN_SERVICE_STATUS_OFF} at first.</li>
 *       <li>then close connections.</li>
 *   </lu>
 *
 * @author tsui
 * @version $Id: ScheduledDisconnectStrategy.java, v 0.1 2017-02-21 14:14 tsui Exp $
 */
public class ScheduledDisconnectStrategy implements ConnectionMonitorStrategy {
    private static final Logger     logger                 = BoltLoggerFactory
                                                               .getLogger("CommonDefault");

    /** the connections threshold of each {@link Url#uniqueKey} */
    private static final int        CONNECTION_THRESHOLD   = ConfigManager.conn_threshold();

    /** fresh select connections to be closed  要关闭的新选择连接 key是poolvalue */
    private Map<String, Connection> freshSelectConnections = new ConcurrentHashMap<String, Connection>();

    /** Retry detect period for ScheduledDisconnectStrategy*/
    private static int              RETRY_DETECT_PERIOD    = ConfigManager.retry_detect_period();

    /** random */
    private Random                  random                 = new Random();

    /**
     * Filter connections to monitor
     * 筛选服务可用的连接和服务不可用的连接
     * @param connections
     */
    @Override
    public Map<String, List<Connection>> filter(List<Connection> connections) {
        List<Connection> serviceOnConnections = new ArrayList<Connection>();
        List<Connection> serviceOffConnections = new ArrayList<Connection>();
        Map<String, List<Connection>> filteredConnections = new ConcurrentHashMap<String, List<Connection>>();

        for (Connection connection : connections) {
            String serviceStatus = (String) connection.getAttribute(Configs.CONN_SERVICE_STATUS);
            if (serviceStatus != null) {//wuzl 不明白为啥不判断状态 是因为只有服务不可用的才打标签吗?
                if (connection.isInvokeFutureMapFinish()
                    && !freshSelectConnections.containsValue(connection)) {
                	//有连接状态 且执行完所有任务 并且不是要关闭的新选择连接
                    serviceOffConnections.add(connection);
                }
            } else {
                serviceOnConnections.add(connection);
            }
        }

        filteredConnections.put(Configs.CONN_SERVICE_STATUS_ON, serviceOnConnections);
        filteredConnections.put(Configs.CONN_SERVICE_STATUS_OFF, serviceOffConnections);
        return filteredConnections;
    }

    /**
     * Monitor connections and close connections with status is off
     * 监控连接并关闭一些关闭状态的练级
     * @param connPools
     */
    @Override
    public void monitor(Map<String, RunStateRecordedFutureTask<ConnectionPool>> connPools) {
        try {
            if (null != connPools && !connPools.isEmpty()) {
                Iterator<Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>>> iter = connPools
                    .entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>> entry = iter
                        .next();
                    String poolKey = entry.getKey();
                    //获取连接池  实际是线程跑完的结果
                    ConnectionPool pool = FutureTaskUtil.getFutureTaskResult(entry.getValue(),
                        logger);

                    List<Connection> connections = pool.getAll();
                    Map<String, List<Connection>> filteredConnectons = this.filter(connections);
                    List<Connection> serviceOnConnections = filteredConnectons
                        .get(Configs.CONN_SERVICE_STATUS_ON);
                    List<Connection> serviceOffConnections = filteredConnectons
                        .get(Configs.CONN_SERVICE_STATUS_OFF);
                    if (serviceOnConnections.size() > CONNECTION_THRESHOLD) {
                    	//随机设置一个服务不可用 等待关闭
                        Connection freshSelectConnect = serviceOnConnections.get(random
                            .nextInt(serviceOnConnections.size()));
                        freshSelectConnect.setAttribute(Configs.CONN_SERVICE_STATUS,
                            Configs.CONN_SERVICE_STATUS_OFF);
                        //弹出上一轮随机要关闭的服务
                        Connection lastSelectConnect = freshSelectConnections.remove(poolKey);
                        freshSelectConnections.put(poolKey, freshSelectConnect);
                        //把上轮认为要关闭的插入到关闭列表中 需要等待任务都执行完
                        closeFreshSelectConnections(lastSelectConnect, serviceOffConnections);

                    } else {
                        if (freshSelectConnections.containsKey(poolKey)) {
                            Connection lastSelectConnect = freshSelectConnections.remove(poolKey);
                            closeFreshSelectConnections(lastSelectConnect, serviceOffConnections);
                        }
                        if (logger.isInfoEnabled()) {
                            logger
                                .info(
                                    "the size of serviceOnConnections [{}] reached CONNECTION_THRESHOLD [{}].",
                                    serviceOnConnections.size(), CONNECTION_THRESHOLD);
                        }
                    }
                    //循环关闭不可用连接
                    for (Connection offConn : serviceOffConnections) {
                        if (offConn.isFine()) {
                            offConn.close();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("ScheduledDisconnectStrategy monitor error", e);
        }
    }

    /**
     * close the connection of the fresh select connections
     *
     * @param lastSelectConnect
     * @param serviceOffConnections
     * @throws InterruptedException
     */
    private void closeFreshSelectConnections(Connection lastSelectConnect,
                                             List<Connection> serviceOffConnections)
                                                                                    throws InterruptedException {
        if (null != lastSelectConnect) {
            if (lastSelectConnect.isInvokeFutureMapFinish()) {
                serviceOffConnections.add(lastSelectConnect);
            } else {
                Thread.sleep(RETRY_DETECT_PERIOD);
                if (lastSelectConnect.isInvokeFutureMapFinish()) {
                    serviceOffConnections.add(lastSelectConnect);
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("Address={} won't close at this schedule turn",
                            RemotingUtil.parseRemoteAddress(lastSelectConnect.getChannel()));
                    }
                }
            }
        }
    }
}
