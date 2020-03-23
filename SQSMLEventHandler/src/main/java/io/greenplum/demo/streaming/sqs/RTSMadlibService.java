package io.greenplum.demo.streaming.sqs;
/**********************************************************************************************
 Copyright 2020 VMWare Inc
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 *********************************************************************************************/

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
@Component
public class RTSMadlibService {
    private RestTemplate restTemplate;

    public RTSMadlibService() {
        RestTemplateBuilder restTemplateBuilder = new RestTemplateBuilder();
        restTemplate = restTemplateBuilder.errorHandler(new RestControlErrorHandler()).build();
    }

    List<Map<String, Object>> process(String transaction, Context context) {
        List<Map<String, Object>> predResults = null;
        try {
            String mlflowEndpoint = System.getenv("RTSMADlib_ENDPOINT");
            context.getLogger().log("<<<<INVOKING RTSMADlib >>>> " + mlflowEndpoint);
            context.getLogger().log(transaction);
            ObjectMapper om = new ObjectMapper();
            Map<String, Object> payload = new HashMap<String, Object> ();
            payload = om.readValue(transaction, new TypeReference<Map<String, Object>>() {});
            ResponseEntity<List> ref = restTemplate.postForEntity(mlflowEndpoint, payload,
                    List.class);
            predResults = ref.getBody();
        } catch (Throwable t) {
            throw new RuntimeException("Exception while invoking the RTSMADlib service.", t);
        }
        return predResults;
    }

}
