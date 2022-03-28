package com.maersk.eacloud.controller;

import com.maersk.eacloud.kafka.KafkaProducer;
import com.maersk.kafkautility.service.AzureBlobService;
import com.microsoft.azure.storage.StorageException;
import net.apmoller.ohm.adapter.avro.model.EventNotificationsAdapterModel;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Collections;


@RestController
public class TestController<T extends Serializable> {

    @Autowired
    KafkaProducer producer;

    @Autowired
    AzureBlobService <T> azureBlobService;

    @PostMapping(value = "/send/kafka/message")
    public String sendKafkaMessage(@RequestBody String message) throws URISyntaxException, InvalidKeyException, StorageException {
       // producer.sendMessage(message, "corId");
        producer.publishMessageToKafka(message, "corId");
        return "Success";
    }

    @PostMapping(value = "/send/avro/message")
    public String postAvroMessage(@RequestBody String message, @RequestParam String corId) throws JSONException, URISyntaxException, InvalidKeyException, StorageException {
        JSONObject response = new JSONObject(message);
        EventNotificationsAdapterModel avro= EventNotificationsAdapterModel.newBuilder()
                .setResponse(response.get("response").toString())
                .setCorrelationId("corId").setMessageType("xml").setMessageId("dfdf-dfd")
                .setSourceSystem("docbroker").setResponseConsumers(Collections.emptyList()).build();
        //producer.sendMessage(avro, corId);
        producer.sendMessageToKafka(avro, "corId");
        return "Success";
    }

    /*@PostMapping (value = "/blob")
    public Map<String, T> blobStorage(@RequestBody String message) throws URISyntaxException, InvalidKeyException, StorageException, IOException {
     return azureBlobService.storePayloadToBlob((T) message);
    }*/

   /* @GetMapping(value = "/blob")
    public String getPayloadFromStorage(@RequestParam String filePath) throws URISyntaxException, InvalidKeyException, StorageException {
        var payload = azureBlobService.getPayloadFromBlob(filePath);
        return payload.toString();
    }*/
}
