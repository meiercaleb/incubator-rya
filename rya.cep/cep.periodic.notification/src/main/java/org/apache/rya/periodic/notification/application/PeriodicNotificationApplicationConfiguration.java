package org.apache.rya.periodic.notification.application;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;

import jline.internal.Preconditions;

public class PeriodicNotificationApplicationConfiguration extends AccumuloRdfConfiguration {

    public static String FLUO_APP_NAME = ConfigUtils.FLUO_APP_NAME;
    public static String FLUO_TABLE_NAME = "fluo.table.name";
    public static String KAFKA_BOOTSTRAP_SERVERS = ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static String NOTIFICATION_TOPIC = "notification.topic";
    public static String NOTIFICATION_GROUP_ID = "notification.group.id";
    public static String NOTIFICATION_CLIENT_ID = "notification.client.id";
    public static String EXPORT_TOPIC = "export.topic";
    public static String EXPORT_GROUP_ID = "export.group.id";
    public static String EXPORT_CLIENT_ID = "export.client.id";
    public static String COORDINATOR_THREADS = "coordinator.threads";
    public static String PRODUCER_THREADS = "producer.threads";
    public static String EXPORTER_THREADS = "exporter.threads";
    public static String PROCESSOR_THREADS = "processor.threads";
    public static String PRUNER_THREADS = "pruner.threads";
    
    public PeriodicNotificationApplicationConfiguration() {}
    
    public PeriodicNotificationApplicationConfiguration(Properties props) {
       super(fromProperties(props));
       setFluoAppName(props.getProperty(FLUO_APP_NAME));
       setFluoTableName(props.getProperty(FLUO_TABLE_NAME));
       setBootStrapServers(props.getProperty(KAFKA_BOOTSTRAP_SERVERS));
       setNotificationClientId(props.getProperty(NOTIFICATION_CLIENT_ID, "consumer0"));
       setNotificationTopic(props.getProperty(NOTIFICATION_TOPIC, "notifications"));
       setNotificationGroupId(props.getProperty(NOTIFICATION_GROUP_ID, "group0"));
       setExportTopic(props.getProperty(EXPORT_TOPIC, "export"));
       setExportClientId(props.getProperty(EXPORT_CLIENT_ID, "consumer0"));
       setExportGroupId(props.getProperty(EXPORT_GROUP_ID, "group0"));
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
    
    public void setExportTopic(String exportTopic) {
        set(EXPORT_TOPIC, Preconditions.checkNotNull(exportTopic));
    }
    
    public void setExportGroupId(String exportGroupId) {
        set(EXPORT_GROUP_ID, Preconditions.checkNotNull(exportGroupId));
    }
    
    public void setExportClientId(String exportClientId) {
        set(EXPORT_GROUP_ID, Preconditions.checkNotNull(exportClientId));
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
    
    public String getExportTopic() {
        return get(EXPORT_TOPIC, "export");
    }
    
    public String getExportGroupId() {
        return get(EXPORT_GROUP_ID, "group0");
    }
    
    public String getExportClientId() {
        return get(EXPORT_CLIENT_ID, "consumer0");
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
