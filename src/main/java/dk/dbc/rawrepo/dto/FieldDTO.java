/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.List;

public class FieldDTO {

    private String name;
    private String indicators;
    private List<SubfieldDTO> subfields;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndicators() {
        return indicators;
    }

    public void setIndicators(String indicators) {
        this.indicators = indicators;
    }

    public List<SubfieldDTO> getSubfields() {
        return subfields;
    }

    public void setSubfields(List<SubfieldDTO> subfields) {
        this.subfields = subfields;
    }

}
