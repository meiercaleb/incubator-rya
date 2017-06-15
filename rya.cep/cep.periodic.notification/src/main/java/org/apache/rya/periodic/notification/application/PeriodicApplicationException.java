package org.apache.rya.periodic.notification.application;

public class PeriodicApplicationException extends Exception {

    private static final long serialVersionUID = 1L;

    public PeriodicApplicationException(String message) {
        super(message);
    }
    
    public PeriodicApplicationException(String message, Throwable t) {
        super(message, t);
    }
}
