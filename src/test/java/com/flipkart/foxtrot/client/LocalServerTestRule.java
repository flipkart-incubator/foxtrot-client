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

package com.flipkart.foxtrot.client;

import com.google.common.collect.ImmutableMap;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class LocalServerTestRule implements TestRule {
    private static final Logger logger = LoggerFactory.getLogger(LocalServerTestRule.class.getSimpleName());

    private final ImmutableMap<String, HttpRequestHandler> handlers;
    private final TestHostPort hostPort;

    public LocalServerTestRule(TestHostPort hostPort, ImmutableMap<String, HttpRequestHandler> handlers) {
        this.handlers = handlers;
        this.hostPort = hostPort;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                LocalTestServer localTestServer = new LocalTestServer(null, null);
                for(Map.Entry<String, HttpRequestHandler> handler : handlers.entrySet()) {
                    localTestServer.register(handler.getKey(), handler.getValue());
                }
                localTestServer.start();
                logger.info("Started test server");
                try {
                    hostPort.setHostName(localTestServer.getServiceAddress().getHostName());
                    hostPort.setPort(localTestServer.getServiceAddress().getPort());
                    base.evaluate();
                } finally {
                    localTestServer.stop();
                    logger.info("Stopped test server");
                }
            }
        };
    }
}
