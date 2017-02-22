package cep.periodic.notification.twill;

import java.util.UUID;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner.LiveInfo;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.yarn.YarnTwillRunnerService;

public class AddDeleteNotificationClient {

    public static void main(String[] args) {

        String zkStr = args[0];
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        TwillRunnerService runner = new YarnTwillRunnerService(yarnConfiguration, zkStr);
        runner.start();
        System.out.println("Looking for running instances of " + NotificationRunnable.class.getSimpleName());
        Iterable<TwillController> controllers = runner.lookup(NotificationRunnable.class.getSimpleName());
        System.out.println("Looking for any running instances");
        Iterable<LiveInfo> info = runner.lookupLive();
        for (LiveInfo i : info) {
            System.out.println("LiveInfo is " + i.getApplicationName());
        }

        TwillController cont = null;
        for (TwillController controller : controllers) {
            System.out.println("Found controller.");
            cont = controller;
            break;
        }

        if (cont != null) {
            String id = "notification_" + UUID.randomUUID().toString();
            System.out.println("Adding notification: " + id);
            cont.sendCommand(Command.Builder.of("add:" + id + ":1:seconds").build());

            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Deleting notification: " + id);
            cont.sendCommand(Command.Builder.of("delete:" + id).build());
        } else {
            System.exit(0);
        }

    }

}
