package com.maersk.eacloud.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.util.HashMap;
import java.util.Map;

public class KafkaConfig implements KafkaListenerConfigurer {
	
	@Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
	@Value("${spring.kafka.consumer.offset-auto-reset:latest}")
    private String consumerOffsetAutoReset;
	@Value("${spring.kafka.consumer.max-poll-records:10}")
    private String consumerMaxPollRecords;
	@Value("${spring.kafka.consumer.group-id}")
    private String consumerGroup;
	
	private final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();

	@Override
	public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
		//
		
	}
	
	@Bean
    public ConsumerFactory<String, ConsumerRecord> consumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerOffsetAutoReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerMaxPollRecords);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public StringJsonMessageConverter stringJsonMessageConverter(ObjectMapper mapper) {
        return new StringJsonMessageConverter(mapper);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ConsumerRecord> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ConsumerRecord> factory = new ConcurrentKafkaListenerContainerFactory();

        factory.setMessageConverter(stringJsonMessageConverter(new ObjectMapper()));
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.setStatefulRetry(true);
        return factory;
    }
    
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<String, String>(producerFactory());
    }

}
