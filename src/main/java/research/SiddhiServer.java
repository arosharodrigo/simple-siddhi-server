package research;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;
import org.wso2.carbon.databridge.core.exception.DataBridgeException;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import research.consumer.EventReceiver;

import java.util.HashMap;
import java.util.Map;

public class SiddhiServer implements WrapperListener {

    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    private ExecutionPlanRuntime executionPlanRuntimeHE;
    private InputHandler inputHandler;
    private InputHandler inputHandlerHE;

    public static void main(String[] args) throws Exception {
        WrapperManager.start(new SiddhiServer(), args);
    }

    public Integer start(String[] strings) {
        System.out.println("=================================================================================");
        System.out.println("==========================Starting Simple Siddhi Server==========================");
        System.out.println("=================================================================================");
        Properties.loadProperties();

        // To avoid exception xml parsing error occur for java 8
        System.setProperty("org.xml.sax.driver", "com.sun.org.apache.xerces.internal.parsers.SAXParser");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory","com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory","com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        // Creating Siddhi Manager
        siddhiManager = new SiddhiManager();

        String executionPlan = Properties.PROP.getProperty("siddhi.query1");
        String executionPlanHE = Properties.PROP.getProperty("siddhi.query2");

        //Generating runtime
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntimeHE = siddhiManager.createExecutionPlanRuntime(executionPlanHE);

        //Adding callback to retrieve output events from query
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        executionPlanRuntimeHE.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        //Retrieving InputHandler to push events into Siddhi
        inputHandler = executionPlanRuntime.getInputHandler("inputFilterStream");
        inputHandlerHE = executionPlanRuntimeHE.getInputHandler("inputHEFilterStream");
        Map<String, InputHandler> inputHandlers = new HashMap<String, InputHandler>();
        inputHandlers.put("inputFilterStream", inputHandler);
        inputHandlers.put("inputHEFilterStream", inputHandlerHE);

        //Starting event processing
        executionPlanRuntime.start();
        executionPlanRuntimeHE.start();

        EventReceiver eventReceiver = new EventReceiver(inputHandlers);
        String host = Properties.PROP.getProperty("databridge.host");
        int port = Integer.parseInt(Properties.PROP.getProperty("databridge.receiver.port"));
        String protocol = Properties.PROP.getProperty("databridge.protocol");
        try {
            eventReceiver.start(host, port, protocol);
        } catch (DataBridgeException e) {
            e.printStackTrace();
        } catch (StreamDefinitionStoreException e) {
            e.printStackTrace();
        }

//        DummyPublisher dummyPublisher = new DummyPublisher(inputHandler);
//        dummyPublisher.init();

        return null;
    }

    public int stop(int i) {
        System.out.println("=================================================================================");
        System.out.println("==========================Stopping Simple Siddhi Server==========================");
        System.out.println("=================================================================================");

        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

        return 0;
    }

    public void controlEvent(int i) {

    }
}
