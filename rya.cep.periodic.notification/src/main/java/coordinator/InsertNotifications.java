package coordinator;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

import com.google.common.base.Preconditions;

import api.Notification;

public class InsertNotifications {
	
	public void insert(FluoClient client, Notification notification, Column column) {
		Preconditions.checkNotNull(client);
		Preconditions.checkNotNull(notification);
		
		insert(client, Collections.singleton(notification), column);
	}
	
	public void insert(FluoClient client, Collection<Notification> notifications, Column column) {
		try(Transaction tx = client.newTransaction()) {
			for(Notification notification: notifications) {
				insert(tx, notification.getId(), notification.getMessage(), column);
			}
		}
	}
	
	private void insert(Transaction tx, String id, Optional<String> message, Column column) {
		if(message.isPresent()) {
			String messageString = message.get();
			tx.set(Bytes.of(id), column, Bytes.of(messageString));
		} else {
			tx.set(Bytes.of(id), column, Bytes.of(new byte[0]));
		}
	}
}
