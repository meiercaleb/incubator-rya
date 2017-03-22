package org.apache.rya.spark.query;

public class SparkRyaQueryException extends Exception {

    private static final long serialVersionUID = -7829908667751362999L;

    public SparkRyaQueryException() {
        super();
    }
    
    public SparkRyaQueryException(String message) {
        super(message);
    }
    
    public SparkRyaQueryException(String message, Throwable throwable) {
        super(message, throwable);
    }
    
    public SparkRyaQueryException(Throwable throwable) {
        super(throwable);
    }
    
}
