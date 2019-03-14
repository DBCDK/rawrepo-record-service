/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

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

    @Override
    public String toString() {
        return "ParamsValidation{" +
                "errors=" + errors +
                '}';
    }
}
