package com.maersk.eacloud.kafka;

import com.maersk.kafkautility.service.AzureBlobService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Objects;

@Service
public class KafkaConsumer<T extends Serializable> {

	@Autowired
	AzureBlobService<T> azureBlobService;

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

	@KafkaListener(topics = {"${kafka.notification.topic}"})
	public void listen(ConsumerRecord<String, String> consumerRecord,
					   @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition
					  , Acknowledgment ack)
	{
		logger.info("partition: {}", partition);

		try {
            if (Objects.nonNull(consumerRecord.value()))
			{
				//T payload = azureBlobService.readPayloadFromBlob(consumerRecord.value());
				//T payload = azureBlobService.getPayloadFromBlob((T)consumerRecord.value(), isLargePayload);
				logger.info("Kafka payload: {}", consumerRecord.value());
				/*EventNotificationsAdapterModel eventModel = (EventNotificationsAdapterModel) payload;
				var jsonPayload = new JsonConverter().convertToJson(eventModel.getResponse());
				logger.info("JSON payload after conversion: {}", jsonPayload);*/
			}
			ack.acknowledge();
		} catch (Exception e)
		{
			logger.error("Exception while reading Kafka event", e);
			ack.acknowledge();
		}
	}

}
