/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AppendCallbackTest {

    AppendMessageCallback callback;

    CommitLog.MessageExtBatchEncoder batchEncoder = new CommitLog.MessageExtBatchEncoder(10 * 1024 * 1024);

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("user.home") + File.separator + "unitteststore");
        messageStoreConfig.setStorePathCommitLog(System.getProperty("user.home") + File.separator + "unitteststore" + File.separator + "commitlog");
        //too much reference
        DefaultMessageStore messageStore = new DefaultMessageStore(messageStoreConfig, null, null, null);
        CommitLog commitLog = new CommitLog(messageStore);
        callback = commitLog.new DefaultAppendMessageCallback(1024);
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(System.getProperty("user.home") + File.separator + "unitteststore"));
    }

    @Test
    public void testAppendMessageBatchEndOfFile() throws Exception {
        List<Message> messages = new ArrayList<>();
        String topic = "test-topic";
        int queue = 0;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody("body".getBytes());
            msg.setTopic(topic);
            msg.setTags("abc");
            messages.add(msg);
        }
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 123));
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 124));
        messageExtBatch.setBody(MessageDecoder.encodeMessages(messages));

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));
        ByteBuffer buff = ByteBuffer.allocate(1024 * 10);
        //encounter end of file when append half of the data
        AppendMessageResult result = callback.doAppend(0, buff, 1000, messageExtBatch);
        assertEquals(AppendMessageStatus.END_OF_FILE, result.getStatus());
        assertEquals(0, result.getWroteOffset());
        assertEquals(0, result.getLogicsOffset());
        assertEquals(1000, result.getWroteBytes());
        assertEquals(8, buff.position()); //write blank size and magic value

        assertTrue(result.getMsgId().length() > 0); //should have already constructed some message ids
    }

    @Test
    public void testAppendMessageBatchSucc() throws Exception {
        List<Message> messages = new ArrayList<>();
        String topic = "test-topic";
        int queue = 0;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody("body".getBytes());
            msg.setTopic(topic);
            msg.setTags("abc");
            messages.add(msg);
        }
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 123));
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 124));
        messageExtBatch.setBody(MessageDecoder.encodeMessages(messages));

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));
        ByteBuffer buff = ByteBuffer.allocate(1024 * 10);
        AppendMessageResult allresult = callback.doAppend(0, buff, 1024 * 10, messageExtBatch);

        assertEquals(AppendMessageStatus.PUT_OK, allresult.getStatus());
        assertEquals(0, allresult.getWroteOffset());
        assertEquals(0, allresult.getLogicsOffset());
        assertEquals(buff.position(), allresult.getWroteBytes());

        assertEquals(messages.size(), allresult.getMsgNum());

        Set<String> msgIds = new HashSet<>();
        for (String msgId : allresult.getMsgId().split(",")) {
            assertEquals(32, msgId.length());
            msgIds.add(msgId);
        }
        assertEquals(messages.size(), msgIds.size());

        List<MessageExt> decodeMsgs = MessageDecoder.decodes((ByteBuffer) buff.flip());
        assertEquals(decodeMsgs.size(), decodeMsgs.size());
        long queueOffset = decodeMsgs.get(0).getQueueOffset();
        long storeTimeStamp = decodeMsgs.get(0).getStoreTimestamp();
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(messages.get(i).getTopic(), decodeMsgs.get(i).getTopic());
            assertEquals(new String(messages.get(i).getBody()), new String(decodeMsgs.get(i).getBody()));
            assertEquals(messages.get(i).getTags(), decodeMsgs.get(i).getTags());

            assertEquals(messageExtBatch.getBornHostNameString(), decodeMsgs.get(i).getBornHostNameString());

            assertEquals(messageExtBatch.getBornTimestamp(), decodeMsgs.get(i).getBornTimestamp());
            assertEquals(storeTimeStamp, decodeMsgs.get(i).getStoreTimestamp());
            assertEquals(queueOffset++, decodeMsgs.get(i).getQueueOffset());
        }

    }

}
