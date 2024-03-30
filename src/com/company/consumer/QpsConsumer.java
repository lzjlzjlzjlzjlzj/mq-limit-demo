package com.company.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;

public class QpsConsumer {
    static String qpsQueueName="qpsQueue";

    public static QpsConfig config=new QpsConfig();

    //qps数量配置类
    public static class QpsConfig{
        private Integer configQps=20;
        private Integer consumerNum=5;
        private Integer configExecuteTime=500;
        private Integer ackTime=1000;

        public void setConfigQps(Integer num){
            this.configQps=num;
        }

        public Integer getBasicQos(){
            return configQps>consumerNum?configQps/consumerNum:1;
        }

        public Integer getRealAckTime(){
            return ackTime*consumerNum/configQps;
        }

        public Integer getRealExecuteTime(){
            return (configExecuteTime-getRealAckTime()<0)?configExecuteTime:configExecuteTime-getRealAckTime();
        }

        public String toString(){
            return "config={getBasicQos:"+getBasicQos()+";getRealAckTime:"+getRealAckTime()+";getRealExecuteTime:"+getRealExecuteTime()+"}";
        }
    }

    /**
     * 监听qps配置修改队列
     */
    public static void listenQpsQueue( Connection connection,Channel channel) throws IOException {
        Channel channel1 = null;
        try {
            channel1 = connection.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        channel1.queueDeclare(qpsQueueName, true, false, false, null);
        DefaultConsumer consumer = new DefaultConsumer(channel1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(LocalDateTime.now()+" 【QpsConsumer】 【RECEIVED】 message: " + message);
                Integer qpsNum=Integer.valueOf(message);
                config.setConfigQps(qpsNum);
                System.out.println("qps配置为："+QpsConsumer.config.toString());
                channel.basicQos(QpsConsumer.config.getBasicQos());
            }

        };
        // 注册消费者以接收消息
        channel1.basicConsume(qpsQueueName, true, consumer);
    }



}
