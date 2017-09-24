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

package research.publisher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.siddhi.core.event.Event;
import research.util.Properties;
import research.util.Util;

public class EventPublisher {

    private static Log log = LogFactory.getLog(EventPublisher.class);

    private DataPublisher dataPublisher;

    public void init() {
        AgentHolder.setConfigPath(Util.getDataAgentConfigPath());
        Util.setTrustStoreParams();

        String username = Properties.PROP.getProperty("databridge.publisher.username");
        String password = Properties.PROP.getProperty("databridge.publisher.password");
        String host = Properties.PROP.getProperty("databridge.publisher.host");
        int port = Integer.parseInt(Properties.PROP.getProperty("databridge.publisher.port"));
        String protocol = Properties.PROP.getProperty("databridge.protocol");

        //create data publisher
        try {
            dataPublisher = new DataPublisher(protocol, "tcp://" + host + ":" + port, null, username, password);
        } catch (DataEndpointAgentConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        } catch (DataEndpointConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointAuthenticationException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        }
        log.info("Event publisher started");
    }

    public void publish(String streamId, long timeStamp, Event[] inEvents, Event[] removeEvents) {
        for(Event event : inEvents) {
            org.wso2.carbon.databridge.commons.Event modifiedEvent =
                    new org.wso2.carbon.databridge.commons.Event(streamId, timeStamp, null, null, event.getData());
            dataPublisher.publish(modifiedEvent);
        }
    }

    public void stop() {
        try {
            dataPublisher.shutdownWithAgent();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        }
    }
}
