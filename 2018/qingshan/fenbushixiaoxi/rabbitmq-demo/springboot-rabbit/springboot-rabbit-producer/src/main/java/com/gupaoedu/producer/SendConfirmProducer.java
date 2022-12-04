package com.gupaoedu.producer;

import com.alibaba.fastjson.JSONObject;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * @program: springboot-rabbit
 * @description:
 * @author: gyg
 * @create: 2020-11-23 00:14
 **/
public class SendConfirmProducer implements RabbitTemplate.ConfirmCallback {
    @Autowired
    RabbitTemplate rabbitTemplate;

    @Transactional(rollbackFor = Exception.class)
    public void send(JSONObject jsonObject){
        String jsonString = jsonObject.toJSONString();
        System.out.println("jsonString:" + jsonString);
        String paymentId = jsonObject.getString("paymentId");
        // 封装消息
        Message message = MessageBuilder.withBody(jsonString.getBytes())
                .setContentType(MessageProperties.CONTENT_TYPE_JSON).setContentEncoding("utf-8").setMessageId(paymentId)
                .build();
        // 构建回调返回的数据（消息id）
        this.rabbitTemplate.setMandatory(true);
        this.rabbitTemplate.setConfirmCallback(this);
        CorrelationData correlationData = new CorrelationData(jsonString);
        rabbitTemplate.convertAndSend("integral_exchange_name", "integralRoutingKey", message, correlationData);
    }

    //生产消息确认机制 生产者往服务器端发送消息的时候，采用应答机制(确认消息不丢失)
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        String jsonString  = correlationData.getId();
        System.out.println("消息id:" + correlationData.getId());
        if(ack){
            System.out.println("消息发送确认成功");
        }else {
            //消息发送失败就递归进入重试机制
            JSONObject jsonObject = JSONObject.parseObject(jsonString);
            send(jsonObject);
            System.out.println("消息发送确认失败:" + cause);
        }
    }

}
    
    