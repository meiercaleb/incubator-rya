package org.apache.rya.periodic.notification.twill;

import java.io.PrintWriter;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;

public class TwillClient {

    // private static final Logger LOG =
    // LoggerFactory.getLogger(TwillClient.class);

    public static void main(String[] args) {

        String zkStr = args[0];
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        TwillRunnerService runner = new YarnTwillRunnerService(yarnConfiguration, zkStr);
        runner.start();

        TwillController controller = runner.prepare(new NotificationRunnable()).withApplicationArguments("notification")
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out))).start();
        

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //
        // Runtime.getRuntime().addShutdownHook(new Thread() {
        // @Override
        // public void run() {
        // try {
        // Futures.getUnchecked(controller.terminate());
        // } finally {
        // runner.stop();
        // }
        // }
        // });
        //
        // try {
        // controller.awaitTerminated();
        // } catch (ExecutionException e) {
        // e.printStackTrace();
        // }
    }

}
