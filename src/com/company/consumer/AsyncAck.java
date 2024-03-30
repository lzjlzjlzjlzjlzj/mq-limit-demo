package com.company.consumer;


import com.company.producer.AckProducer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ack异步确认，实现延迟限制
 */
public class AsyncAck {

    private static ExecutorService executorService= Executors.newFixedThreadPool(5);


    public static void ack(String consumerName, Channel channel, String message, DefaultConsumer consumer, long deliveryTag, Integer realAckTime){

        try {
            Thread.sleep(realAckTime);
            AckProducer.sendMsg(channel,"{ deliveryTag:"+deliveryTag+";consumer:"+consumerName+";realAckTime:"+realAckTime+";+message:"+message+"}");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //发送ack
                    try {
                        consumer.getChannel().basicAck(deliveryTag,false);
                        //System.out.println(LocalDateTime.now()+" 【"+consumerName+"】 【ACK】 message: " + message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
        //System.out.println(LocalDateTime.now()+" 【"+consumerName+"】 异步ack发布成功: " + message);
    }


    public static void handleMsg(String consumerName,String message,Integer realExecuteTime){
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(realExecuteTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(LocalDateTime.now()+" 【"+consumerName+"】 【OVER】 message: " + message);
            }
        });
    }
}
