/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.exception;

import javax.ws.rs.WebApplicationException;

public class WebApplicationInvalidInputException extends WebApplicationException {

    public WebApplicationInvalidInputException(String msg) {
        super(msg);
    }

}
