/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.exception;

public class InternalServerException extends Exception {

    public InternalServerException() {
        super();
    }

    public InternalServerException(String msg, Throwable t) {
        super(msg, t);
    }
}
