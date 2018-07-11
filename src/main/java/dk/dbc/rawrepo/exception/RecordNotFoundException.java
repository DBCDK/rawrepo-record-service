/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.exception;

public class RecordNotFoundException extends Exception {

    public RecordNotFoundException() {
        super();
    }

    public RecordNotFoundException(String msg) {
        super(msg);
    }

}
