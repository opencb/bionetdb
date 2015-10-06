package org.opencb.bionetdb.core.exceptions;

/**
 * Created by pfurio on 10/9/15.
 */
public class BioNetDBException extends Exception {

    public BioNetDBException(String message) {
        super(message);
    }

    public BioNetDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public BioNetDBException(Throwable cause) {
        super(cause);
    }

}
