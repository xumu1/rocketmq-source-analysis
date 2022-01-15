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
package org.apache.rocketmq.namesrv.kvconfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.namesrv.NamesrvController;

// KV 配置管理器
public class KVConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvController namesrvController;

    // 读写锁，限制访问 configTable
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable =
            new HashMap<String, HashMap<String, String>>();

    public KVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    // 加载 JSON 文件
    public void load() {
        String content = null;
        try {
            // 将 fileName 中的配置转为 String
            content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        } catch (IOException e) {
            log.warn("Load KV config table exception", e);
        }
        if (content != null) {
            // KVConfigSerializeWrapper 继承了 RemotingSerializable，fromJson 就是将 JsonString 解析为
            // KVConfigSerializeWrapper 对象
            KVConfigSerializeWrapper kvConfigSerializeWrapper =
                    KVConfigSerializeWrapper.fromJson(content, KVConfigSerializeWrapper.class);
            if (null != kvConfigSerializeWrapper) {
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load KV config table OK");
            }
        }
    }

    // 写入配置
    public void putKVConfig(final String namespace, final String key, final String value) {
        // configTable getOrCreate namespace 指代的 HashMap
        HashMap<String, String> kvTable = this.configTable.get(namespace);
        if (null == kvTable) {
            kvTable = new HashMap<String, String>();
            this.configTable.put(namespace, kvTable);
            log.info("putKVConfig create new Namespace {}", namespace);
        }

        final String prev = kvTable.put(key, value);
        if (null != prev) {
            log.info("putKVConfig update config item, Namespace: {} Key: {} Value: {}",
                    namespace, key, value);
        } else {
            log.info("putKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                    namespace, key, value);
        }

        // 持久化
        this.persist();
    }

    // 持久化 
    public void persist() {
        // 序列化为一个 JSON 字符串
        KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
        kvConfigSerializeWrapper.setConfigTable(this.configTable);

        String content = kvConfigSerializeWrapper.toJson();
        if (null != content) {
            // 将 JSON 字符串落盘
            try {
                MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // 删除 configTable 指定命名空间下的 key
    public void deleteKVConfig(final String namespace, final String key) {
        HashMap<String, String> kvTable = this.configTable.get(namespace);
        if (null != kvTable) {
            String value = kvTable.remove(key);
            log.info("deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                    namespace, key, value);
        }
        this.persist();
    }

    // 获取指定 namespace 的 KVList
    public byte[] getKVListByNamespace(final String namespace) {
        HashMap<String, String> kvTable = this.configTable.get(namespace);
        if (null != kvTable) {
            KVTable table = new KVTable();
            table.setTable(kvTable);
            // 拿到 namespace 对应的 HashMap，然后序列化
            return table.encode();
        }
        return null;
    }

    // 获取执行命名空间中 key 对应的 value
    public String getKVConfig(final String namespace, final String key) {
        HashMap<String, String> kvTable = this.configTable.get(namespace);
        if (null != kvTable) {
            return kvTable.get(key);
        }
        return null;
    }

    // 定期打印所有 kv pair
    public void printAllPeriodically() {
        log.info("configTable SIZE: {}", this.configTable.size());
        Iterator<Entry<String, HashMap<String, String>>> it =
                this.configTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<String, String>> next = it.next();
            Iterator<Entry<String, String>> itSub = next.getValue().entrySet().iterator();
            while (itSub.hasNext()) {
                Entry<String, String> nextSub = itSub.next();
                log.info("configTable NS: {} Key: {} Value: {}", next.getKey(), nextSub.getKey(),
                        nextSub.getValue());
            }
        }
    }
}
