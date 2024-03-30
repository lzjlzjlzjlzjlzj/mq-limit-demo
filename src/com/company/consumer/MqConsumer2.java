package com.company.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Scanner;

/**
 * 消费者
 */
public class MqConsumer2 {


        public static void main(String[] args) throws Exception {
            Scanner scanner=new Scanner(System.in);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            String queueName = "testQueue";
            String qpsQueueName="qpsQueue";
            Channel channel1 = connection.createChannel();
            channel1.queueDeclare(queueName, true, false, false, null);
            System.out.println("qps配置为："+QpsConsumer.config.toString());
            // 设置prefetch_count为1，表示每次发送一条消息，才会发送下一条消息
            channel1.basicQos(QpsConsumer.config.getBasicQos());
            //acl配置队列
            Channel ackChanel = connection.createChannel();
            ackChanel.queueDeclare("ackQueue", true, false, false, null);
            Consumer consumer2 = new DefaultConsumer(channel1) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(LocalDateTime.now()+" 【consumer2】 【RECEIVED】 message: " + message);
                    AsyncAck.handleMsg("consumer2",message,QpsConsumer.config.getRealExecuteTime());
                    AsyncAck.ack("consumer2",ackChanel,message,this,envelope.getDeliveryTag(),QpsConsumer.config.getRealAckTime());
                }
            };
            //注册配置消费者修改qps配置
            QpsConsumer.listenQpsQueue(connection,channel1);
            // 注册消费者以接收消息
            channel1.basicConsume(queueName,false,consumer2);
            //主线程不结束
            while (true){

            }
        }
}
