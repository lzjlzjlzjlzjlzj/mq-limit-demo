package com.company.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Scanner;

public class MqProducer {
    private final static String QUEUE_NAME = "testQueue";

    static String qpsQueueName="qpsQueue";

    public static void main(String[] argv) throws Exception {
        //主线程
        Thread main=Thread.currentThread();
        // 创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // 创建连接
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 声明一个队列，如果队列不存在会被创建
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        //qps配置队列
        Channel qpsChanel = connection.createChannel();
        qpsChanel.queueDeclare(qpsQueueName, true, false, false, null);
        String message = "Hello World! id=";
        int i=1;
        Scanner scanner=new Scanner(System.in);
        while (true){
            System.out.println("请开始发送消息 输入命令 1=【发送一条】  2=【批量发送】 3=【id清零】 4=【修改qps数量】  5=【结束程序】");
            int ml= scanner.nextInt();
            if(ml==1){
               sendMsg(channel,QUEUE_NAME,message+i);
               i++;
            }
            if(ml==2){
                System.out.println("请输入批量发送的条数");
                int size= scanner.nextInt();
                for (int j=0;j<size;j++){
                    sendMsg(channel,QUEUE_NAME,message+i);
                    i++;
                }
            }
            if(ml==3){
               i=1;
            }
            if(ml==4){
                System.out.println("请输入要配置的qps数量");
                int qps= scanner.nextInt();
                sendMsg(channel,qpsQueueName,qps+"");
            }
            if(ml==5){
                break;
            }
        }
        channel.close();
        connection.close();
    }


    public static void sendMsg(Channel channel,String queueName,String msg){
        // 发布消息到队列中
        try {
            channel.basicPublish("", queueName, null, msg.getBytes());
            System.out.println(LocalDateTime.now()+" [x] Sent '" + msg + "'");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
