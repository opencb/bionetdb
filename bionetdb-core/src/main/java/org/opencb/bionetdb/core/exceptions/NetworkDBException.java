package org.opencb.bionetdb.core.exceptions;

/**
 * Created by pfurio on 10/9/15.
 */
public class NetworkDBException extends Exception {

    public NetworkDBException(String message) {
        super(message);
    }

    public NetworkDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public NetworkDBException(Throwable cause) {
        super(cause);
    }

}
