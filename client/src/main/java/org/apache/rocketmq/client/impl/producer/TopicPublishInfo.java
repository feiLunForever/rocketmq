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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    /**
     * 首次生成一个随机数，后面线性增加，顺序选择
     * 比如 2 3 0 1
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private TopicRouteData topicRouteData; // 路由数据

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        // lastBrokerName 代表上次选择的 MessageQueue 所在的 Broker，
        // 并且它只会在第一次投递失败之后的后续重试流程中有值
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            // 递失败可能代表着，单台 Broker 的网络、或者所在机器出了问题，
            // 那么下次重新选择时，如果再选到同一台 Broker 投递大概率还是会继续失败，
            // 所以为了尽可能地让 Message 投递成功，会选择另一台 Broker 进行投递。
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 举个具体点的例子，假设最初生成的随机值是 10，并总共有 4 个 MessageQueue，那么首次计算出来的下标就是 10 % 4 = 2，首次选择的结果就是下标为 2 的 MessageQueue。
     *
     * 同理，第二次选择时由于 this.threadLocalIndex.get(); 拿到的值一定不为空，所以就会在之前的值上自增，那么第二次的计算逻辑就是 11 % 4 = 3，即下标为 3 的 MessageQueue。
     *
     * 相信大家也发现了，基于这种选择的算法，MessageQueue 列表中的每个 MessageQueue 是按着顺序挨个被选择出来的，啥意思呢？还是拿上面这个例子说明一下，首次选择出来的下标是 2，那么后续在正常情况下被选择到的 MessageQueue 依次是：
     *
     * 2 3 0 1
     * 2 3 0 1
     * 2 3 0 1
     * .......
     *
     * 一个线性轮询的负载均衡算法，可以将流量均匀地分发给不同的 MessageQueue，而 MessageQueue 分布在不同的 Broker 上，这样也达到了对最终 Message 存储的负载均衡，避免造成数据倾斜。
     * @return
     */
    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet(); // 拿到 sendWhichQueue
        int pos = Math.abs(index) % this.messageQueueList.size(); // 对 messageQueue 列表长度取余
        if (pos < 0) // 计算出来的 pos 如果小于 0, 就给个兜底 = 0
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
