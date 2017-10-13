package research;

import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import research.consumer.EventReceiver;
import research.publisher.EventPublisher;
import research.util.Properties;

import java.util.HashMap;
import java.util.Map;

public class SiddhiServer implements WrapperListener {

    static final Logger log = Logger.getLogger(SiddhiServer.class);

    private SiddhiManager siddhiManager;

    private ExecutionPlanRuntime executionPlanRuntime;
    private ExecutionPlanRuntime executionPlanRuntimeHE;
    private ExecutionPlanRuntime executionPlanRuntimeEmails;
    private ExecutionPlanRuntime executionPlanRuntimeEmailsHE;

    private InputHandler inputHandler;
    private InputHandler inputHandlerHE;
    private InputHandler inputEmailsHandler;
    private InputHandler inputEmailsHandlerHE;

    private EventReceiver eventReceiver;
    private EventPublisher eventPublisher;

    private static final String INPUT_FILTER_STREAM_ID = "inputFilterStream";
    private static final String INPUT_FILTER_STREAM_ID_VERSION = "inputFilterStream:1.0.0";
    private static final String INPUT_HE_FILTER_STREAM_ID = "inputHEFilterStream";
    private static final String INPUT_HE_FILTER_STREAM_ID_VERSION = "inputHEFilterStream:1.0.0";
    private static final String INPUT_EMAILS_STREAM_ID = "inputEmailsStream";
    private static final String INPUT_EMAILS_STREAM_ID_VERSION = "inputEmailsStream:1.0.0";
    private static final String OUTPUT_EMAILS_STREAM_ID_VERSION = "outputEmailsStream:1.0.0";
    private static final String INPUT_HE_EMAILS_STREAM_ID = "inputHEEmailsStream";
    private static final String INPUT_HE_EMAILS_STREAM_ID_VERSION = "inputHEEmailsStream:1.0.0";
    private static final String OUTPUT_HE_EMAILS_STREAM_ID_VERSION = "outputHEEmailsStream:1.0.0";

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
        String executionPlanEmails = Properties.PROP.getProperty("siddhi.query3");
        String executionPlanEmailsHE = Properties.PROP.getProperty("siddhi.query4");

        //Generating runtime
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntimeHE = siddhiManager.createExecutionPlanRuntime(executionPlanHE);
        executionPlanRuntimeEmails = siddhiManager.createExecutionPlanRuntime(executionPlanEmails);
        executionPlanRuntimeEmailsHE = siddhiManager.createExecutionPlanRuntime(executionPlanEmailsHE);

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
        executionPlanRuntimeEmails.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                eventPublisher.publish(OUTPUT_EMAILS_STREAM_ID_VERSION, timeStamp, inEvents, removeEvents);
            }
        });
        executionPlanRuntimeEmailsHE.addCallback("query4", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                eventPublisher.publish(OUTPUT_HE_EMAILS_STREAM_ID_VERSION, timeStamp, inEvents, removeEvents);
            }
        });

        //Retrieving InputHandler to push events into Siddhi
        inputHandler = executionPlanRuntime.getInputHandler(INPUT_FILTER_STREAM_ID);
        inputHandlerHE = executionPlanRuntimeHE.getInputHandler(INPUT_HE_FILTER_STREAM_ID);
        inputEmailsHandler = executionPlanRuntimeEmails.getInputHandler(INPUT_EMAILS_STREAM_ID);
        inputEmailsHandlerHE = executionPlanRuntimeEmailsHE.getInputHandler(INPUT_HE_EMAILS_STREAM_ID);

        Map<String, InputHandler> inputHandlers = new HashMap<String, InputHandler>();
        inputHandlers.put(INPUT_FILTER_STREAM_ID_VERSION, inputHandler);
        inputHandlers.put(INPUT_HE_FILTER_STREAM_ID_VERSION, inputHandlerHE);
        inputHandlers.put(INPUT_EMAILS_STREAM_ID_VERSION, inputEmailsHandler);
        inputHandlers.put(INPUT_HE_EMAILS_STREAM_ID_VERSION, inputEmailsHandlerHE);

        //Starting event processing
        executionPlanRuntime.start();
        executionPlanRuntimeHE.start();
        executionPlanRuntimeEmails.start();
        executionPlanRuntimeEmailsHE.start();

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
        executionPlanRuntimeHE.shutdown();
        executionPlanRuntimeEmails.shutdown();
        executionPlanRuntimeEmailsHE.shutdown();
        //Shutting down Siddhi
        siddhiManager.shutdown();
        eventReceiver.stop();
        eventPublisher.stop();

        return 0;
    }

    public void controlEvent(int i) {

    }
}
