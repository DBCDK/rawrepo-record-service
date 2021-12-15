package dk.dbc.rawrepo.exception;

import javax.ws.rs.WebApplicationException;

public class WebApplicationInvalidInputException extends WebApplicationException {

    public WebApplicationInvalidInputException(String msg) {
        super(msg);
    }

}
