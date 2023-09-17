package com.example.kafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.example.kafka.domain.EventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.example.kafka.domain.LibraryEvent;
import com.example.kafka.service.KafkaLibraryServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;

@RestController
@RequestMapping("kafka")
@Slf4j
public class KafkaLibraryController {
	
	@Autowired
	private KafkaLibraryServiceImpl kafkaServiceImpl;
	
	@RequestMapping(value = "/test", method = RequestMethod.GET)
	public String test()
	{
		return "Tested Ok!";
	}

	@RequestMapping(value = "/publish", method = RequestMethod.POST)
	public ResponseEntity<LibraryEvent>publishLibraryEvent(@RequestBody LibraryEvent event) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
		//kafkaServiceImpl.sendMsg(event);
		log.info("before kafka template call");
		//kafkaServiceImpl.sendMsgSynchronous(event);
		event.setEventType(EventType.NEW);
		kafkaServiceImpl.sendMsgProducerRecord(event);
		log.info("After kafka template call");
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
	}

	@RequestMapping(value = "/publish", method = RequestMethod.PUT)
	public ResponseEntity<?>putLibraryEvent(@RequestBody LibraryEvent event) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
		//kafkaServiceImpl.sendMsg(event);
		if(event.getLibraryEventId()==null)
		{
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("The event Id can't be null");
		}
		log.info("before kafka template call");
		//kafkaServiceImpl.sendMsgSynchronous(event);
		event.setEventType(EventType.UPDATE);
		kafkaServiceImpl.sendMsgProducerRecord(event);
		log.info("After kafka template call");
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
	}
}
