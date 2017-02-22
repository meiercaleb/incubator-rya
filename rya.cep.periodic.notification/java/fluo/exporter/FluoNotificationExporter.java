package fluo.exporter;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.data.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import api.LifeCycle;
import api.Notification;
import api.NotificationExporter;
import api.PeriodicNotificationCoordinator;
import scheduled.executor.coordinator.InsertNotifications;

public class FluoNotificationExporter implements NotificationExporter, LifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(FluoNotificationExporter.class);
	private FluoClient client;
	private PeriodicNotificationCoordinator coord;
	private ExecutorService service;
	private int numThreads;
	
	public FluoNotificationExporter(PeriodicNotificationCoordinator coord, FluoClient client, int numThreads) {
		this.client = client;
		this.numThreads = numThreads;
		this.coord = coord;
	}
	
	
	@Override
	public void start() {
		service = Executors.newFixedThreadPool(numThreads);
		
		for(int i = 0; i < numThreads; i++) {
			service.submit(new ExporterRunnable(coord, this));
		}
	}

	@Override
	public void stop() {
		try {
			client.close();
			service.shutdown();
			if(!service.awaitTermination(60, TimeUnit.SECONDS)) {
				LOG.info("Executor did not terminate in specified amount of time.");
				List<Runnable> droppedTasks =  service.shutdownNow();
				LOG.info("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
			}
		} catch (InterruptedException e) {
			LOG.info("Shutdown of Executor service interrupted.");
			throw new RuntimeException(e);
		}
	}


	@Override
	public void exportNotification(Notification notification) {
		//TODO The column name might be subject to change
		new InsertNotifications().insert(client, notification, new Column("PeriodicQuery", "Notification"));
	}
	
	
	private static class ExporterRunnable implements Runnable {

		private FluoNotificationExporter exporter;
		private PeriodicNotificationCoordinator coord;
		
		public ExporterRunnable(PeriodicNotificationCoordinator coord, FluoNotificationExporter exporter) {
			this.exporter = exporter;
			this.coord = coord;
		}
		
		@Override
		public void run() {
			while(true) {
				exporter.exportNotification(coord.getNextPeriodicNotification());
			}
		}
		
	}

}
