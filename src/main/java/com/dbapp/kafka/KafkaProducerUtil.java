package com.dbapp.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author: jackson.tang
 * @version: 1.0
 * @Date: Created in 2018/2/24 18:49
 * @Modified: kafka version 0.10.0.1
 */
class KafkaProducerUtil {


    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerUtil.class);
    private final KafkaProducer<String, String> producer;
    private List<ProducerRecord<String, String>> messageList = null;

    public KafkaProducerUtil(String kafka_bro)
    {

        Properties props = new Properties();
        props.put("bootstrap.servers", kafka_bro);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);

    }

    public void produce(List<String> source_data, String topic)
    {
        for (String raw_data : source_data)
        {
            ProducerRecord<String, String > record = new ProducerRecord<>(
                    topic, raw_data);
            this.producer.send(record);
        }
        source_data.clear();

    }

    public void produce(String msg, String topic)
    {
        ProducerRecord<String, String > record = new ProducerRecord<>(topic, msg);
        try
        {
            this.producer.send(record);
        }
        catch (Exception e)
        {
            LOG.error(e.getMessage(), e);
        }
    }



    public static void main(String[] args)
    {
        KafkaProducerUtil k = new KafkaProducerUtil("bd238:9092,bd236:9092,bd237:9092");
        List<String> a = new ArrayList();
        a.add("11");
        k.produce(a, "test");
        k.produce("test", "test");
        System.out.println("done");
    }

}

