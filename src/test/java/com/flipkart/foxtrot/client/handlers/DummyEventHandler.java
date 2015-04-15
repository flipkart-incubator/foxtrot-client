/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.client.handlers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.client.Document;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyEventHandler implements HttpRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(DummyEventHandler.class.getSimpleName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        try {
            BasicHttpEntityEnclosingRequest post = (BasicHttpEntityEnclosingRequest)request;
            List<Document> documents = mapper.readValue(EntityUtils.toByteArray(post.getEntity()), new TypeReference<List<Document>>() {});
            counter.addAndGet(documents.size());
            logger.info("Received {} documents.", documents.size());
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
        response.setStatusCode(201);
    }

    public AtomicInteger getCounter() {
        return counter;
    }
}
