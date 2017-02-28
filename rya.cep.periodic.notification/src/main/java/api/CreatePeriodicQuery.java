package api;

import org.apache.fluo.api.client.FluoClient;

public class CreatePeriodicQuery {

    private FluoClient fluoClient;
    private PeriodicNotificationClient notifyClient;
    
    public CreatePeriodicQuery(FluoClient client1, PeriodicNotificationClient client2) {
        fluoClient = client1;
        notifyClient = client2;
    }
    
    
    
}
