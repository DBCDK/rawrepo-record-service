package dk.dbc.rawrepo.dump;

import java.util.List;

public class ParamsValidation {

    private List<ParamsValidationItem> errors;

    public List<ParamsValidationItem> getErrors() {
        return errors;
    }

    public void setErrors(List<ParamsValidationItem> errors) {
        this.errors = errors;
    }
}
