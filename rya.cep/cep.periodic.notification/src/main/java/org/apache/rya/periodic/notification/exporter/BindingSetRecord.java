package org.apache.rya.periodic.notification.exporter;

import org.openrdf.query.BindingSet;

import com.google.common.base.Objects;

public class BindingSetRecord {

    private BindingSet bs;
    private String topic;
    
    public BindingSetRecord(BindingSet bs, String topic) {
        this.bs = bs;
        this.topic = topic;
    }
    
    public BindingSet getBindingSet() {
        return bs;
    }
    
    public String getTopic() {
        return topic;
    }
    
    @Override 
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        
        if(o instanceof BindingSetRecord) {
            BindingSetRecord record = (BindingSetRecord) o;
            return Objects.equal(this.bs, record.bs)&&Objects.equal(this.topic,record.topic);
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(bs, topic);
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append("Binding Set Record \n").append("  Topic: " + topic + "\n").append("  BindingSet: " + bs + "\n")
                .toString();
    }
    
}
