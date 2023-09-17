package com.example.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaLibraryConfiguration {
	
	@Bean
	public  ProducerFactory<Integer, String> producerFactory()
	{
		Map<String, Object>prop= new HashMap<>();
		
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		prop.put(ProducerConfig.ACKS_CONFIG, "all");
		prop.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
		prop.put(ProducerConfig.RETRIES_CONFIG, 3);

		return new DefaultKafkaProducerFactory<>(prop);
	}
	
	@Bean
	public  KafkaTemplate<Integer ,String> libraryTemplate()
	{
		
		return new KafkaTemplate<>(producerFactory());
	}
}
