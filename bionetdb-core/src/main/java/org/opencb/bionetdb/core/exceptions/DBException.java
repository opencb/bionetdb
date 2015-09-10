package org.opencb.bionetdb.core.exceptions;

/**
 * Created by pfurio on 10/9/15.
 */
public class DBException extends Exception {

    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBException(Throwable cause) {
        super(cause);
    }

}
