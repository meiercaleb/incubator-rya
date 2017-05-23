package org.apache.rya.indexing.pcj.storage;

public class PeriodicQueryStorageException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of {@link PeriodicQueryStorageException}.
     *
     * @param message - Describes why the exception is being thrown.
     */
    public PeriodicQueryStorageException(final String message) {
        super(message);
    }

    /**
     * Constructs an instance of {@link PeriodicQueryStorageException}.
     *
     * @param message - Describes why the exception is being thrown.
     * @param cause - The exception that caused this one to be thrown.
     */
    public PeriodicQueryStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
}
