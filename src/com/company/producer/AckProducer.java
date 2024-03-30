package com.company.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Scanner;

public class AckProducer {
    private final static String QUEUE_NAME = "ackQueue";


    public static void sendMsg(Connection connection,String ackMsg) throws Exception {
        //acl配置队列
        Channel ackChanel = connection.createChannel();
        ackChanel.queueDeclare(QUEUE_NAME, true, false, false, null);
        sendMsg(ackChanel,ackMsg);
    }


    public static void sendMsg(Channel channel,String msg){
        // 发布消息到队列中
        try {
            channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
            System.out.println(LocalDateTime.now()+" [x] Sent ackMsg:'" + msg + "'");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
