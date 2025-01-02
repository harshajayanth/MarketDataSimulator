package com.fnhackathon.MarketDataSimulator.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fnhackathon.MarketDataSimulator.Model.TradeOrderDTO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class KafkaProducerService {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
	
	@Value("${app.dataset}")
	private String dataset;
	
	@Value("${kafka.serverconfig}")
	private String ServerConfig;
	
	@Value("${kafka.topic}")
	private String TopicName;
	
	private static int partition=0;
	
	public String produceOrders() {
		
		TradeOrderDTO dto=new TradeOrderDTO();
		
		ObjectMapper objectMapper=new ObjectMapper();
		InputStream inputStream=getClass().getClassLoader().getResourceAsStream(dataset);
		
		try {
			List<TradeOrderDTO> orders = objectMapper.readValue(inputStream, new TypeReference<List<TradeOrderDTO>>() {});
			return sendtoKafka(orders);
		} catch (IOException e) {
			return e.toString();
			}
	}


	public String sendtoKafka(List<TradeOrderDTO> orders) {
		
	
		Properties props=new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ServerConfig);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		
		try(KafkaProducer<String,String> producer=new KafkaProducer<>(props)){
			
			ObjectMapper objectMapper=new ObjectMapper();
			
			orders.forEach(order-> {
				
				String orderJson;
				
				try {
					orderJson = objectMapper.writeValueAsString(order);
					    String uniqueKey = "order-" + order.hashCode();
						ProducerRecord<String,String> record=new ProducerRecord(TopicName,partition,uniqueKey,orderJson);
						producer.send(record);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			});
			
			return "Record Produced in Kafka";
		}catch(Exception e) {
			return "Error in Producing Records in Kafka" + e.toString();
		}
		
		
	}
	
	public void StopKafka() {
		
		Properties props=new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ServerConfig);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		KafkaProducer<String,String> producer=new KafkaProducer<>(props);
		producer.close();
		
	}

}
