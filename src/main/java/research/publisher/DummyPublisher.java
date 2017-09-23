package research.publisher;

import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DummyPublisher {

    private InputHandler inputHandler;

    public DummyPublisher(InputHandler inputHandler) {
        this.inputHandler = inputHandler;
    }

    public void init() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    //Sending events to Siddhi
                    inputHandler.send(new Object[]{"IBM", 700f, 100l});
                    inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
                    inputHandler.send(new Object[]{"GOOG", 50f, 30l});
                    inputHandler.send(new Object[]{"IBM", 76.6f, 400l});
                    inputHandler.send(new Object[]{"WSO2", 45.6f, 50l});
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            }
        }, 5000, 5000, TimeUnit.MILLISECONDS);
    }



}
