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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

// 一个消费分组对应一个 MQConsumerInner，存储在 MQClientInstance.consumerTable 中
public class DefaultMQPullConsumerImpl implements MQConsumerInner {
    private final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final RPCHook rpcHook;
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private OffsetStore offsetStore;
    private RebalanceImpl rebalanceImpl = new RebalancePullImpl(this);

    public DefaultMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer, final RPCHook rpcHook) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
        this.rpcHook = rpcHook;
    }

    // 注册消费消息的钩子函数
    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    // 创建 topic
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.isRunning();
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    // 这个命名太秀了。。以 check 打头会好点吧。。
    private void isRunning() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer is not in running status, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    // 拉取消费 offset
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        this.isRunning();
        return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    }

    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        this.isRunning();
        if (null == topic) {
            throw new IllegalArgumentException("topic is null");
        }

        // 获取 processQueueTable
        ConcurrentMap<MessageQueue, ProcessQueue> mqTable = this.rebalanceImpl.getProcessQueueTable();
        // filter 出对应 topic 的 MQ
        Set<MessageQueue> mqResult = new HashSet<MessageQueue>();
        for (MessageQueue mq : mqTable.keySet()) {
            if (mq.getTopic().equals(topic)) {
                mqResult.add(mq);
            }
        }

        return parseSubscribeMessageQueues(mqResult);
    }

    // 拉取发布的 MQ
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    // 拉取订阅的 MQ
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        this.isRunning();
        // check if has info in memory, otherwise invoke api.
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        }

        return parseSubscribeMessageQueues(result);
    }

    // 过滤 namespace，得到用户定义的 topic
    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> queueSet) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue messageQueue : queueSet) {
            // 过滤掉 namespace，得到用户定义的 topic
            String userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(),
                    this.defaultMQPullConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
        }
        return resultQueues;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subExpression, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
    }

    public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, messageSelector, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
    }

    // 获取 SubscriptionData
    private SubscriptionData getSubscriptionData(MessageQueue mq, String subExpression)
            throws MQClientException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        try {
            // 切分 subExpression，填充 tagsSet 和 codeSet，用于后续过滤
            return FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                    mq.getTopic(), subExpression);
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }
    }

    private SubscriptionData getSubscriptionData(MessageQueue mq, MessageSelector messageSelector)
            throws MQClientException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        try {
            return FilterAPI.build(mq.getTopic(),
                    messageSelector.getExpression(), messageSelector.getExpressionType());
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }
    }

    // 同步 pull
    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, boolean block,
                                    long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.isRunning();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

        long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        // 调用 pullAPIWrapper 中的接口获取 PullResult
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
                mq,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                isTagType ? 0L : subscriptionData.getSubVersion(),
                offset,
                maxNums,
                sysFlag,
                0,
                this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
                timeoutMillis,
                CommunicationMode.SYNC,
                null  // 同步的话，不需要 PullCallback
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        //If namespace is not null , reset Topic without namespace.
        this.resetTopic(pullResult.getMsgFoundList());
        if (!this.consumeMessageHookList.isEmpty()) {
            ConsumeMessageContext consumeMessageContext = null;
            consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setNamespace(defaultMQPullConsumer.getNamespace());
            consumeMessageContext.setConsumerGroup(this.groupName());
            consumeMessageContext.setMq(mq);
            consumeMessageContext.setMsgList(pullResult.getMsgFoundList());
            consumeMessageContext.setSuccess(false);
            this.executeHookBefore(consumeMessageContext);
            consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
            consumeMessageContext.setSuccess(true);
            this.executeHookAfter(consumeMessageContext);
        }
        return pullResult;
    }

    // 删除 topic 的 namespace 前缀
    public void resetTopic(List<MessageExt> msgList) {
        if (null == msgList || msgList.size() == 0) {
            return;
        }

        // If namespace not null, reset Topic without namespace.
        // 如果 namespace 不为 null，删除 topic 的 namespace 前缀
        for (MessageExt messageExt : msgList) {
            if (null != this.getDefaultMQPullConsumer().getNamespace()) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultMQPullConsumer.getNamespace()));
            }
        }

    }

    // 补充 rebalanceImpl 中 topic 对应的 SubscriptionData
    public void subscriptionAutomatically(final String topic) {
        if (!this.rebalanceImpl.getSubscriptionInner().containsKey(topic)) {
            try {
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                        topic, SubscriptionData.SUB_ALL);
                this.rebalanceImpl.subscriptionInner.putIfAbsent(topic, subscriptionData);
            } catch (Exception ignore) {
            }
        }
    }

    // 取消订阅 topic
    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    @Override
    public String groupName() {
        return this.defaultMQPullConsumer.getConsumerGroup();
    }

    // 执行消费前的钩子
    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    // 执行消费后的钩子
    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPullConsumer.getMessageModel();
    }

    // pull 消费模式
    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    // 为所有的 topic 生成 SubscriptionData
    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> result = new HashSet<SubscriptionData>();

        Set<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
        if (topics != null) {
            synchronized (topics) {
                for (String t : topics) {
                    SubscriptionData ms = null;
                    try {
                        ms = FilterAPI.buildSubscriptionData(this.groupName(), t, SubscriptionData.SUB_ALL);
                    } catch (Exception e) {
                        log.error("parse subscription error", e);
                    }
                    ms.setSubVersion(0L);
                    result.add(ms);
                }
            }
        }

        return result;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    // 持久化消费 offset
    @Override
    public void persistConsumerOffset() {
        try {
            this.isRunning();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPullConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    // 更新 topic 对应的 MQ set
    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    // 查看是否需要更新
    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        pull(mq, subExpression, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback,
                     long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
        this.pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, false, timeout);
    }

    public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
                     PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        pull(mq, messageSelector, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums,
                     PullCallback pullCallback,
                     long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
        this.pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, false, timeout);
    }

    // 异步拉取
    private void pullAsyncImpl(
            final MessageQueue mq,
            final SubscriptionData subscriptionData,
            final long offset,
            final int maxNums,
            final PullCallback pullCallback,
            final boolean block,
            final long timeout) throws MQClientException, RemotingException, InterruptedException {
        this.isRunning();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        if (null == pullCallback) {
            throw new MQClientException("pullCallback is null", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        try {
            int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

            long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

            boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
            this.pullAPIWrapper.pullKernelImpl(
                    mq,
                    subscriptionData.getSubString(),
                    subscriptionData.getExpressionType(),
                    isTagType ? 0L : subscriptionData.getSubVersion(),
                    offset,
                    maxNums,
                    sysFlag,
                    0,
                    this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(),
                    timeoutMillis,
                    CommunicationMode.ASYNC,
                    new PullCallback() {

                        @Override
                        public void onSuccess(PullResult pullResult) {
                            PullResult userPullResult = DefaultMQPullConsumerImpl.this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
                            resetTopic(userPullResult.getMsgFoundList());
                            pullCallback.onSuccess(userPullResult);
                        }

                        @Override
                        public void onException(Throwable e) {
                            pullCallback.onException(e);
                        }
                    });
        } catch (MQBrokerException e) {
            throw new MQClientException("pullAsync unknow exception", e);
        }
    }

    // block 模式的同步 pull
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    // block 模式的异步 pull
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
                                    PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
        this.pullAsyncImpl(mq, subscriptionData, offset, maxNums, pullCallback, true,
                this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
            throws MQClientException, InterruptedException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, this.defaultMQPullConsumer.getConsumerGroup());
    }

    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    // 消费者消费失败，将消息写回 broker
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            // broker 的地址
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

            if (UtilAll.isBlank(consumerGroup)) {
                consumerGroup = this.defaultMQPullConsumer.getConsumerGroup();
            }

            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, consumerGroup, delayLevel, 3000,
                    this.defaultMQPullConsumer.getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPullConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPullConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(this.defaultMQPullConsumer.getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPullConsumer.getNamespace()));
        }
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                this.copySubscription();

                if (this.defaultMQPullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPullConsumer.changeInstanceNameToPID();
                }

                this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPullConsumer, this.rpcHook);

                this.rebalanceImpl.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPullConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPullConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPullConsumer.getConsumerGroup(), isUnitMode());
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultMQPullConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPullConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPullConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPullConsumer.setOffsetStore(this.offsetStore);
                }

                this.offsetStore.load();

                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;

                    throw new MQClientException("The consumer group[" + this.defaultMQPullConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                mQClientFactory.start();
                log.info("the consumer [{}] start OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

    }

    // 检查配置
    private void checkConfig() throws MQClientException {
        // check consumerGroup
        Validators.checkGroup(this.defaultMQPullConsumer.getConsumerGroup());

        // consumerGroup
        if (null == this.defaultMQPullConsumer.getConsumerGroup()) {
            throw new MQClientException(
                    "consumerGroup is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumerGroup
        if (this.defaultMQPullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "
                            + MixAll.DEFAULT_CONSUMER_GROUP
                            + ", please specify another one."
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // messageModel
        if (null == this.defaultMQPullConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // allocateMessageQueueStrategy
        if (this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                    "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    // 根据 defaultMQPullConsumer 中的 topic 更新 rebalanceImpl 的 SubscriptionData HashMap
    private void copySubscription() throws MQClientException {
        try {
            Set<String> registerTopics = this.defaultMQPullConsumer.getRegisterTopics();
            if (registerTopics != null) {
                for (final String topic : registerTopics) {
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),
                            topic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.isRunning();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public MessageExt viewMessage(String msgId)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.isRunning();
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    public void setPullAPIWrapper(PullAPIWrapper pullAPIWrapper) {
        this.pullAPIWrapper = pullAPIWrapper;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long getConsumerStartTimestamp() {
        return consumerStartTimestamp;
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }
}
