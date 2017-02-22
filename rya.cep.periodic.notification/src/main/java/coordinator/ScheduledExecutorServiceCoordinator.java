package coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import api.Notification;
import api.PeriodicNotificationCoordinator;
import kafka.CommandNotification;
import kafka.CommandNotification.Command;
import kafka.PeriodicNotification;
import kafka.TimestampedNotification;

public class ScheduledExecutorServiceCoordinator implements PeriodicNotificationCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(ScheduledExecutorServiceCoordinator.class);
	private int numThreads;
	private ScheduledExecutorService producerThreadPool;
	private Map<String, ScheduledFuture<?>> serviceMap = new HashMap<>();
	private BlockingQueue<TimestampedNotification> notifications;
	private final ReentrantLock lock = new ReentrantLock(true);

	public ScheduledExecutorServiceCoordinator(int numThreads) {
		this.numThreads = numThreads;
		this.notifications = new LinkedBlockingQueue<>();
	}

	@Override
	public void processNextCommandNotification(CommandNotification notification) {
		lock.lock();
		try {
			processNotification(notification);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public TimestampedNotification getNextPeriodicNotification() {
		try {
			return notifications.take();
		} catch (InterruptedException e) {
			LOG.info("Unable to retrieve next notification.  Process interrupted.");
			throw new RuntimeException(e);
		}
	}

	@Override
	public void start() {
		producerThreadPool = Executors.newScheduledThreadPool(numThreads);
	}

	@Override
	public void stop() {
		producerThreadPool.shutdown();
		LOG.info("Service Executor Shutdown has been called.  Terminating NotificationRunnable");
	}

	private void processNotification(CommandNotification notification) {
		Command command = notification.getCommand();
		Notification periodic = notification.getNotification();
		switch (command) {
		case ADD:
			addNotification(periodic);
			break;
		case DELETE:
			deleteNotification(periodic);
			break;
		}
	}

	private void addNotification(Notification notification) {
		Preconditions.checkArgument(notification instanceof PeriodicNotification);
		PeriodicNotification notify = (PeriodicNotification) notification;
		ScheduledFuture<?> future = producerThreadPool.scheduleAtFixedRate(new NotificationProducer(notify),
				notify.getInitialDelay(), notify.getPeriod(), notify.getPeriodTimeUnit());
		serviceMap.put(notify.getId(), future);
	}

	private boolean deleteNotification(Notification notification) {
		if (serviceMap.containsKey(notification.getId())) {
			ScheduledFuture<?> future = serviceMap.remove(notification.getId());
			future.cancel(true);
			return true;
		}
		return false;
	}

	class NotificationProducer implements Runnable {

		private PeriodicNotification notification;

		public NotificationProducer(PeriodicNotification notification) {
			this.notification = notification;
		}

		public void run() {
			try {
				notifications.put(new TimestampedNotification(notification.getId()));
			} catch (InterruptedException e) {
				LOG.info("Unable to add notification.  Process interrupted. ");
				throw new RuntimeException(e);
			}
		}

	}

}
