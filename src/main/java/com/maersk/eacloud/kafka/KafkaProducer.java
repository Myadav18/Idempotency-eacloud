package com.maersk.eacloud.kafka;

import com.maersk.kafkautility.annotations.RetryHandler;
import com.maersk.kafkautility.exception.MessagePublishException;
import com.maersk.kafkautility.service.*;
import com.microsoft.azure.storage.StorageException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

@Component
@Transactional
public class KafkaProducer<T extends Serializable> {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

	@Value("${kafka.notification.topic}")
	private String producerTopic;

	@Autowired
	private RetryService<T> retryService;

	@Autowired
	private MessagePublishHandler<T> messagePublishHandler;

	@Autowired
	AzureBlobService<T> azureBlobService;

	@Autowired
	KafkaProducerService<T> kafkaProducerService;

//	@RetryHandler
//	@Retryable(value = MessagePublishException.class, maxAttemptsExpression = "${spring.retry.maximum.attempts}")
	public void sendMessage(T message, String docBrokerCorrelationId) {
		logger.info("Publish message: {}", message);
		try {
			ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
			messagePublishHandler.publishOnTopic((ProducerRecord<String, T>) producerRecord);
		}
		catch(Exception ex)
		{
			logger.error("Exception: ", ex);
			logger.info("Retry count: {}", RetrySynchronizationManager.getContext().getRetryCount());
			throw new MessagePublishException(ex.getMessage());
		}
	}


//	@Retryable(value = MessagePublishException.class, maxAttemptsExpression = "${spring.retry.maximum.attempts}")
	public void sendMessageToKafka(T message, String docBrokerCorrelationId) {
		logger.info("Publish message: {}", message);
		try {
			var producerRecord = azureBlobService.storePayloadToBlob(producerTopic, message);
			messagePublishHandler.publishOnTopic(producerRecord);
		}
		catch(Exception ex)
		{
			logger.error("Exception: ", ex);
			throw new MessagePublishException(ex.getMessage());
		}
	}

//	@Retryable(value = MessagePublishException.class, maxAttemptsExpression = "${spring.retry.maximum.attempts}")
	public void publishMessageToKafka(T message, String docBrokerCorrelationId) throws URISyntaxException, InvalidKeyException, StorageException {
		logger.info("Publish message: {}", message);
		String payloadReference = null;
		try {
			//int i = 3/0;
			//payloadReference = azureBlobService.writePayloadToBlob(message);
			ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
			//var producerRecord = kafkaProducerService.getProducerRecord(message);
			messagePublishHandler.publishOnTopic((ProducerRecord<String, T>) producerRecord);
		}
		catch(Exception ex)
		{
			//logger.info("Retry count: {}", RetrySynchronizationManager.getContext().getRetryCount());
			logger.error("Exception: ", ex);

//			azureBlobService.deletePayloadFromBlob(payloadReference);
			throw new MessagePublishException(ex.getMessage());
		}
	}

	@Recover
	public void sendMessageToRetryTopic(RuntimeException runtimeException, T message, T docBrokerCorrelationId) {
		logger.info("Inside recover method with cor Id: {}", docBrokerCorrelationId);
		try
		{
			retryService.sendMessageToRetryTopic(message, docBrokerCorrelationId);
		}
		catch (Exception ex)
		{
			logger.error("Exception in recover method", ex);
		}
	}

	/*private EventNotification buildEventPayload(String payload, String aggregateId)
	{
		Date date = new Date();
		Timestamp timestamp = new Timestamp(date.getTime());
		return EventNotification.builder().instanceId(UUID.randomUUID().toString())
				.aggregateId(aggregateId).eventName("message_publish_success").eventPayload(payload)
				.createTime(timestamp).build();
	}*/

}
