package research;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import research.consumer.EventReceiver;
import research.publisher.EventPublisher;
import research.util.Properties;

import java.util.HashMap;
import java.util.Map;

public class SiddhiServer implements WrapperListener {

    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    private ExecutionPlanRuntime executionPlanRuntimeHE;
    private InputHandler inputHandler;
    private InputHandler inputHandlerHE;

    private EventReceiver eventReceiver;
    private EventPublisher eventPublisher;

    private static final String INPUT_FILTER_STREAM_ID = "inputFilterStream";
    private static final String INPUT_FILTER_STREAM_ID_VERSION = "inputFilterStream:1.0.0";
    private static final String INPUT_HE_FILTER_STREAM_ID = "inputHEFilterStream";
    private static final String INPUT_HE_FILTER_STREAM_ID_VERSION = "inputHEFilterStream:1.0.0";

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
                eventPublisher.publish(INPUT_FILTER_STREAM_ID_VERSION, timeStamp, inEvents, removeEvents);
            }
        });
        executionPlanRuntimeHE.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                eventPublisher.publish(INPUT_HE_FILTER_STREAM_ID_VERSION, timeStamp, inEvents, removeEvents);
            }
        });

        //Retrieving InputHandler to push events into Siddhi
        inputHandler = executionPlanRuntime.getInputHandler(INPUT_FILTER_STREAM_ID);
        inputHandlerHE = executionPlanRuntimeHE.getInputHandler(INPUT_HE_FILTER_STREAM_ID);
        Map<String, InputHandler> inputHandlers = new HashMap<String, InputHandler>();
        inputHandlers.put(INPUT_FILTER_STREAM_ID_VERSION, inputHandler);
        inputHandlers.put(INPUT_HE_FILTER_STREAM_ID_VERSION, inputHandlerHE);

        //Starting event processing
        executionPlanRuntime.start();
        executionPlanRuntimeHE.start();

        eventReceiver = new EventReceiver(inputHandlers);
        eventReceiver.init();

        eventPublisher = new EventPublisher();
        eventPublisher.init();

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
        eventReceiver.stop();
        eventPublisher.stop();

        return 0;
    }

    public void controlEvent(int i) {

    }
}
