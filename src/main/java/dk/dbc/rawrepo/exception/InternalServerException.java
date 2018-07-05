package dk.dbc.rawrepo.exception;

public class InternalServerException extends Exception {

    public InternalServerException() {
        super();
    }

    public InternalServerException(String msg) {
        super(msg);
    }

    public InternalServerException(String msg, Throwable t) {
        super(msg, t);
    }
}
