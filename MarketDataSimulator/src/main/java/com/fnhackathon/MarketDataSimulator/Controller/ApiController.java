package com.fnhackathon.MarketDataSimulator.Controller;

import java.util.List;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fnhackathon.MarketDataSimulator.Model.TradeOrderDTO;
import com.fnhackathon.MarketDataSimulator.Service.KafkaProducerService;

import redis.clients.jedis.Jedis;


@RestController
@RequestMapping("/api")
public class ApiController {
	
	@Value("${redis.port}")
	private int REDIS_PORT;
	
	@Value("${consumer.url}")
	private String CONSUMER_URL;
	
	@Value("${datareader.url}")
	private String DATAREADER_URL;
	
	@Autowired
	private KafkaProducerService kafkaProducerService;
	
	private Jedis getJedis() {
		Jedis jedis=new Jedis("localhost",REDIS_PORT);
		return jedis;
	}

	
	
	private static boolean running=true;
	
	@PostMapping("/start")
	public String StartService() {
		if(running==true) {
			try {
			String ProducerResponse=kafkaProducerService.produceOrders();
			
			RestTemplate restTemplate = new RestTemplate();
			
	        String ConsumeResponse = restTemplate.postForObject(CONSUMER_URL, null, String.class);
	        
	        String RedisResponse=restTemplate.getForObject(DATAREADER_URL,String.class);
	        
	        String Response="MicroService 1 : "+ProducerResponse+"\n"+"MicroService 2 : "+ConsumeResponse+"\n"+"MicroService 3 :\n"+RedisResponse;
	        
			return Response;
			
			}
			catch(Exception e){
				return "Unable to Start Microservices : "+e.toString();
			}
		}
		return "Services Stopped.Please Restart the Server";
	}
	
	
	@PostMapping("/start/produce")
	public String DynamicProducer(@RequestBody TradeOrderDTO dto) {
		
		List<TradeOrderDTO> order=List.of(dto);
			
		try {
		String ProducerResponse=kafkaProducerService.sendtoKafka(order);
		
		RestTemplate restTemplate = new RestTemplate();
		
        String ConsumeResponse = restTemplate.postForObject(CONSUMER_URL, null, String.class);
        
        String RedisResponse=restTemplate.getForObject(DATAREADER_URL,String.class);
        
        String Response="MicroService 1 : "+ProducerResponse+"\n"+"MicroService 2 : "+ConsumeResponse+"\n"+"MicroService 3 : "+RedisResponse;
        
		return Response;
		
		}catch(Exception e){
			return "Unable to Start Microservices : "+e.toString();
		}
	}
	
	@PostMapping("/stop")
	public String StopService() {
		try {
		this.running=false;
		
		Jedis jedis=getJedis();
		jedis.close();
		
		kafkaProducerService.StopKafka();
		
		return "All Services Stopped";
		}
		catch(Exception e) {
			return "Failed To Stop Microservices : " + e.toString();
		}
	}
}
