/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package research.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.Credentials;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.core.AgentCallback;
import org.wso2.carbon.databridge.core.DataBridge;
import org.wso2.carbon.databridge.core.Utils.AgentSession;
import org.wso2.carbon.databridge.core.definitionstore.AbstractStreamDefinitionStore;
import org.wso2.carbon.databridge.core.definitionstore.InMemoryStreamDefinitionStore;
import org.wso2.carbon.databridge.core.exception.DataBridgeException;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import org.wso2.carbon.databridge.core.internal.authentication.AuthenticationHandler;
import org.wso2.carbon.databridge.receiver.binary.conf.BinaryDataReceiverConfiguration;
import org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver;
import org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver;
import org.wso2.carbon.user.api.UserStoreException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import research.util.Properties;
import research.util.Util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EventReceiver {

    private static Log log = LogFactory.getLog(EventReceiver.class);

    private ThriftDataReceiver thriftDataReceiver;
    private BinaryDataReceiver binaryDataReceiver;
    private AbstractStreamDefinitionStore streamDefinitionStore = new InMemoryStreamDefinitionStore();

    private Map<String, InputHandler> inputHandlers;

    public EventReceiver(Map<String, InputHandler> inputHandlers) {
        this.inputHandlers = inputHandlers;
    }

    public void init() {
        String host = Properties.PROP.getProperty("databridge.receiver.host");
        int port = Integer.parseInt(Properties.PROP.getProperty("databridge.receiver.port"));
        String protocol = Properties.PROP.getProperty("databridge.protocol");
        try {
            start(host, port, protocol);
        } catch (DataBridgeException e) {
            e.printStackTrace();
        } catch (StreamDefinitionStoreException e) {
            e.printStackTrace();
        }
    }

    private void start(String host, int receiverPort, String protocol) throws DataBridgeException, StreamDefinitionStoreException {
        Util.setTrustStoreParams();
        Util.setPseudoCarbonHome();
        Util.setKeyStoreParams();

        DataBridge databridge = new DataBridge(new AuthenticationHandler() {
            public boolean authenticate(String userName, String password) {
                return true;
            }

            public String getTenantDomain(String userName) {
                return "carbon.super";
            }

            public int getTenantId(String s) throws UserStoreException {
                return -1234;
            }

            public void initContext(AgentSession agentSession) {

            }

            public void destroyContext(AgentSession agentSession) {

            }

        }, streamDefinitionStore, Util.getDataBridgeConfigPath());

        for (StreamDefinition streamDefinition : Util.loadStreamDefinitions()) {
            streamDefinitionStore.saveStreamDefinitionToStore(streamDefinition, -1234);
            log.info("StreamDefinition of '" + streamDefinition.getStreamId() + "' added to store");
        }

        databridge.subscribe(new AgentCallback() {
            public void definedStream(StreamDefinition streamDefinition, int tenantID) {
                log.info("StreamDefinition " + streamDefinition);
            }

            public void removeStream(StreamDefinition streamDefinition, int tenantID) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void receive(List<Event> eventList, Credentials credentials) {
                for(Event event : eventList) {
                    try {
                        InputHandler inputHandler = inputHandlers.get(event.getStreamId());
                        inputHandler.send(new org.wso2.siddhi.core.event.Event(event.getTimeStamp(), event.getPayloadData()));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        if (protocol.equalsIgnoreCase("binary")) {
            binaryDataReceiver = new BinaryDataReceiver(
                    new BinaryDataReceiverConfiguration(receiverPort + 100, receiverPort), databridge);
            try {
                binaryDataReceiver.start();
            } catch (IOException e) {
                log.error("Error starting binary data receiver: " + e.getMessage(), e);
            }
        } else {
            thriftDataReceiver = new ThriftDataReceiver(receiverPort + 100, receiverPort, databridge);
            thriftDataReceiver.start(host);
        }
        log.info("Event receiver started");
    }

    public void stop() {
        if (thriftDataReceiver != null) {
            thriftDataReceiver.stop();
        }
        if (binaryDataReceiver != null) {
            binaryDataReceiver.stop();
        }
        log.info("Test Server Stopped");
    }
}