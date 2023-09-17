package com.example.kafka.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.example.kafka.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class KafkaLibraryServiceImpl {
	
	Logger logger=LoggerFactory.getLogger(KafkaLibraryServiceImpl.class.getName());
	String topic="library-events";
	@Autowired
	KafkaTemplate<Integer, String> libraryTemplate;
	
	@Autowired
	ObjectMapper mapper;

	//Asynchronous call : Producer does not wait for the kafka send method response.
	public void sendMsg(LibraryEvent event) throws JsonProcessingException 
	{
		Integer key= event.getLibraryEventId();
		String data=mapper.writeValueAsString(event);
		ListenableFuture<SendResult<Integer, String>>responseFuture=libraryTemplate.send(topic,event.getLibraryEventId(), data);
		responseFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handlesuccess(key, data, result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				try {
					handleError(key, data, ex);
				} catch (Throwable e) {
					
					logger.info("exception: {}", e.toString());
				}		
			}
		});
	}

	public void sendMsgProducerRecord(LibraryEvent event) throws JsonProcessingException
	{
		Integer key= event.getLibraryEventId();
		String data=mapper.writeValueAsString(event);
		ProducerRecord<Integer, String> proRcord= buildProducerRecord(topic,key,data);
		ListenableFuture<SendResult<Integer, String>>responseFuture=libraryTemplate.send(proRcord);
		responseFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handlesuccess(key, data, result);

			}

			@Override
			public void onFailure(Throwable ex) {
				try {
					handleError(key, data, ex);
				} catch (Throwable e) {

					logger.info("exception: {}", e.toString());
				}
			}
		});

	}

	private ProducerRecord<Integer, String> buildProducerRecord(String topic, Integer key, String data) {

		return new ProducerRecord<Integer, String>(topic, null, key, data, null);
	}

	//Synchronous call : Producer does wait for the kafka send method response by using the get method.
	public SendResult<Integer, String> sendMsgSynchronous(LibraryEvent event) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
		Integer key = event.getLibraryEventId();
		String data = mapper.writeValueAsString(event);
		SendResult<Integer, String> responseFuture=null;
		try {
			responseFuture = libraryTemplate.send("library-events", event.getLibraryEventId(), data).get(2, TimeUnit.SECONDS);
		}catch(Exception e)
		{
			logger.error("Exception in  synchronous call");
			throw e;
		}
		return responseFuture;
	}

			protected void handleError(Integer key, String data, Throwable ex) throws Throwable {
		logger.info("Error occured while sending the data for key: {} and value: {} and exception: {}", key, data, ex.toString());
		
		throw ex;
		
	}

	protected void handlesuccess(Integer key, String data, SendResult<Integer, String> result) {
		logger.info("message Successfully send for key: {} and value is: {} and the partition is: {}",key, data, result.getRecordMetadata().partition());
	}
	

}
