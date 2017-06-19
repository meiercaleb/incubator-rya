package org.apache.rya.periodic.notification.application;

import java.util.Properties;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;

import jline.internal.Preconditions;

public class PeriodicNotificationApplicationConfiguration extends AccumuloRdfConfiguration {

    public static String FLUO_APP_NAME = "fluo.app.name";
    public static String FLUO_TABLE_NAME = "fluo.table.name";
    public static String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static String NOTIFICATION_TOPIC = "kafka.notification.topic";
    public static String NOTIFICATION_GROUP_ID = "kafka.notification.group.id";
    public static String NOTIFICATION_CLIENT_ID = "kafka.notification.client.id";
    public static String COORDINATOR_THREADS = "cep.coordinator.threads";
    public static String PRODUCER_THREADS = "cep.producer.threads";
    public static String EXPORTER_THREADS = "cep.exporter.threads";
    public static String PROCESSOR_THREADS = "cep.processor.threads";
    public static String PRUNER_THREADS = "cep.pruner.threads";
    
    public PeriodicNotificationApplicationConfiguration() {}
    
    /**
     * Creates an PeriodicNotificationApplicationConfiguration object from a Properties file.  This method assumes
     * that all values in the Properties file are Strings and that the Properties file uses the keys below.
     * See rya.cep/cep.integration.tests/src/test/resources/properties/notification.properties for an example.
     * <br>
     * <ul>
     * <li>"accumulo.auths" - String of Accumulo authorizations. Default is empty String.
     * <li>"accumulo.instance" - Accumulo instance name (required)
     * <li>"accumulo.user" - Accumulo user (required)
     * <li>"accumulo.password" - Accumulo password (required)
     * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.  Default is "rya_"
     * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo instance (required)
     * <li>"fluo.app.name" - Name of Fluo Application (required)
     * <li>"fluo.table.name" - Name of Fluo Table (required)
     * <li>"kafka.bootstrap.servers" - Kafka Bootstrap servers for Producers and Consumers (required)
     * <li>"kafka.notification.topic" - Topic to which new Periodic Notifications are published. Default is "notifications".
     * <li>"kafka.notification.client.id" - Client Id for notification topic.  Default is "consumer0"
     * <li>"kafka.notification.group.id" - Group Id for notification topic.  Default is "group0"
     * <li>"cep.coordinator.threads" - Number of threads used by coordinator. Default is 1.
     * <li>"cep.producer.threads" - Number of threads used by producer.  Default is 1.
     * <li>"cep.exporter.threads" - Number of threads used by exporter.  Default is 1.
     * <li>"cep.processor.threads" - Number of threads used by processor.  Default is 1.
     * <li>"cep.pruner.threads" - Number of threads used by pruner.  Default is 1.
     * </ul>
     * <br>
     * @param props - Properties file containing Accumulo specific configuration parameters
     * @return AccumumuloRdfConfiguration with properties set
     */
    public PeriodicNotificationApplicationConfiguration(Properties props) {
       super(fromProperties(props));
       setFluoAppName(props.getProperty(FLUO_APP_NAME));
       setFluoTableName(props.getProperty(FLUO_TABLE_NAME));
       setBootStrapServers(props.getProperty(KAFKA_BOOTSTRAP_SERVERS));
       setNotificationClientId(props.getProperty(NOTIFICATION_CLIENT_ID, "consumer0"));
       setNotificationTopic(props.getProperty(NOTIFICATION_TOPIC, "notifications"));
       setNotificationGroupId(props.getProperty(NOTIFICATION_GROUP_ID, "group0"));
       setProducerThreads(Integer.parseInt(props.getProperty(PRODUCER_THREADS, "1")));
       setProcessorThreads(Integer.parseInt(props.getProperty(PROCESSOR_THREADS, "1")));
       setExporterThreads(Integer.parseInt(props.getProperty(EXPORTER_THREADS, "1")));
       setPrunerThreads(Integer.parseInt(props.getProperty(PRUNER_THREADS, "1")));
       setCoordinatorThreads(Integer.parseInt(props.getProperty(COORDINATOR_THREADS, "1")));
    }
    
    public void setFluoAppName(String fluoAppName) {
        set(FLUO_APP_NAME, Preconditions.checkNotNull(fluoAppName));
    }
    
    public void setFluoTableName(String fluoTableName) {
       set(FLUO_TABLE_NAME, Preconditions.checkNotNull(fluoTableName)); 
    }
    
    public void setBootStrapServers(String bootStrapServers) {
        set(KAFKA_BOOTSTRAP_SERVERS, Preconditions.checkNotNull(bootStrapServers)); 
    }
    
    public void setNotificationTopic(String notificationTopic) {
        set(NOTIFICATION_TOPIC, Preconditions.checkNotNull(notificationTopic));
    }
    
    public void setNotificationGroupId(String notificationGroupId) {
        set(NOTIFICATION_GROUP_ID, Preconditions.checkNotNull(notificationGroupId));
    }
    
    public void setNotificationClientId(String notificationClientId) {
        set(NOTIFICATION_GROUP_ID, Preconditions.checkNotNull(notificationClientId));
    }
    
    public void setCoordinatorThreads(int threads) {
        setInt(COORDINATOR_THREADS, threads);
    }
    
    public void setExporterThreads(int threads) {
        setInt(EXPORTER_THREADS, threads);
    }
    
    public void setProducerThreads(int threads) {
        setInt(PRODUCER_THREADS, threads);
    }
    
    public void setPrunerThreads(int threads) {
        setInt(PRUNER_THREADS, threads);
    }
    
    public void setProcessorThreads(int threads) {
        setInt(PROCESSOR_THREADS, threads);
    }
    
    public String getFluoAppName() {
        return get(FLUO_APP_NAME);
    }
    
    public String getFluoTableName() {
       return get(FLUO_TABLE_NAME); 
    }
    
    public String getBootStrapServers() {
        return get(KAFKA_BOOTSTRAP_SERVERS); 
    }
    
    public String getNotificationTopic() {
        return get(NOTIFICATION_TOPIC, "notifications");
    }
    
    public String getNotificationGroupId() {
        return get(NOTIFICATION_GROUP_ID, "group0");
    }
    
    public String getNotificationClientId() {
        return get(NOTIFICATION_CLIENT_ID, "consumer0");
    }
    
    public int getCoordinatorThreads() {
        return getInt(COORDINATOR_THREADS, 1);
    }
    
    public int getExporterThreads() {
        return getInt(EXPORTER_THREADS, 1);
    }
    
    public int getProducerThreads() {
        return getInt(PRODUCER_THREADS, 1);
    }
    
    public int getPrunerThreads() {
        return getInt(PRUNER_THREADS, 1);
    }
    
    public int getProcessorThreads() {
        return getInt(PROCESSOR_THREADS, 1);
    }
    
}
