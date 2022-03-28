package com.maersk.eacloud.converter;

import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonConverter {

    private static final Logger log = LoggerFactory.getLogger(JsonConverter.class);

    public String convertToJson(String xmlString) {
        try {
            var json = XML.toJSONObject(xmlString);
            log.info("Json value of response: {}", json);
            return json.toString();
        } catch (Exception e) {
            log.error("error occurred while converting xml to json", e);
        }
        return null;
    }
}
