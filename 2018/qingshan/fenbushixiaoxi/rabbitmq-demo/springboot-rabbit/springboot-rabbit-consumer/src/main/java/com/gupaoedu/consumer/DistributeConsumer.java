package com.gupaoedu.consumer;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.impl.AMQImpl;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import com.rabbitmq.client.Channel;
import java.util.Map;

/**
 * @Author: qingshan
 * @Date: 2018/10/20 17:04
 * @Description: 咕泡学院，只为更好的你
 */
@Component
@PropertySource("classpath:gupaomq.properties")
@RabbitListener(queues = "${com.gupaoedu.distributequeue}", containerFactory="rabbitListenerContainerFactory")
public class DistributeConsumer {

        @RabbitHandler
        public void process(Message message, @Headers Map<String, Object> headers, Channel channel) throws IOException {
            try {
                String messageId = message.getMessageProperties().getMessageId();
                String msg = new String(message.getBody(), "UTF-8");
                JSONObject jsonObject = JSONObject.parseObject(msg);
                String paymentId = jsonObject.getString("paymentId");
                if (StringUtils.isEmpty(paymentId)) {
                    basicNack(message, channel);
                    return;
                }
                // 使用paymentId查询是否已经增加过积分
                //IntegralEntity resultIntegralEntity = integralMapper.findIntegral(paymentId);
//                if (resultIntegralEntity != null) {
//                    log.error(">>>>paymentId:{}已经增加过积分", paymentId);
//                    basicNack(message, channel);
//                    return;
//                }
                Integer userId = jsonObject.getInteger("userId");
                if (userId == null) {
                    basicNack(message, channel);
                    return;
                }
                Long integral = jsonObject.getLong("integral");
                if (integral == null) {
                    return;
                }
//                IntegralEntity integralEntity = new IntegralEntity();
//                integralEntity.setPaymentId(paymentId);
//                integralEntity.setIntegral(integral);
//                integralEntity.setUserId(userId);
//                integralEntity.setAvailability(1);
//                int insertIntegral = integralMapper.insertIntegral(integralEntity);
                //模拟插入数据
                int insertIntegral = 1;
                if (insertIntegral > 0) {
                    // 手动签收消息,通知mq服务器端删除该消息
                    basicNack(message, channel);
                }
            } catch (Exception e) {
                basicNack(message, channel); 		}

        }

            private void basicNack(Message message, Channel channel) throws IOException {
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
            }

}
