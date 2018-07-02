package dk.dbc.rawrepo.dto;

import java.util.List;

public class ContentDTO {

    private List<FieldDTO> fields;
    private String leader;

    public ContentDTO() {
    }

    public List<FieldDTO> getFields() {
        return fields;
    }

    public void setFields(List<FieldDTO> fields) {
        this.fields = fields;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    @Override
    public String toString() {
        return "ContentDTO{" +
                "fields=" + fields +
                ", leader='" + leader + '\'' +
                '}';
    }
}
