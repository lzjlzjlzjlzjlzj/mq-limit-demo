package com.company.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Scanner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
/**
 * 消费者
 */
public class MqConsumer1 {


        public static void main(String[] args) throws Exception {
            Scanner scanner=new Scanner(System.in);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            String queueName = "testQueue";
            Channel channel1 = connection.createChannel();
            channel1.queueDeclare(queueName, true, false, false, null);
            // 设置prefetch_count为1，表示每次发送一条消息，才会发送下一条消息
            System.out.println("qps配置为："+QpsConsumer.config.toString());
            channel1.basicQos(QpsConsumer.config.getBasicQos());
            //acl配置队列
            Channel ackChanel = connection.createChannel();
            ackChanel.queueDeclare("ackQueue", true, false, false, null);
            DefaultConsumer consumer = new DefaultConsumer(channel1) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(LocalDateTime.now()+" 【consumer1】 【RECEIVED】 message: " + message);
                    AsyncAck.handleMsg("consumer1",message,QpsConsumer.config.getRealExecuteTime());
                    AsyncAck.ack("consumer1",ackChanel,message,this,envelope.getDeliveryTag(),QpsConsumer.config.getRealAckTime());
                    this.getChannel().basicQos(QpsConsumer.config.getBasicQos());
                }

            };
            //注册配置消费者修改qps配置
            QpsConsumer.listenQpsQueue(connection,channel1);
            // 注册消费者以接收消息
            channel1.basicConsume(queueName, false, consumer);
            //主线程不结束
            while (true){

            }
        }
}
