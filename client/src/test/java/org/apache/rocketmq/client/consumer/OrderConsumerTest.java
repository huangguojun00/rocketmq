package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class OrderConsumerTest {
    public static void main(String[] args) throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("SyncTopic1ConsumerGroup1");

        //2.指定Nameserver地址
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        //3.订阅主题Topic和Tag
        pushConsumer.subscribe("SyncTopic1", "*");
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //4. 注册消息监听器
        /**
         * 参数一： MessageListenerConcurrently， 会使用多线程，并发消费消息
         */
        pushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt messageExt : msgs) {
                    System.out.println(new java.lang.String(messageExt.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        pushConsumer.start();
    }
}
