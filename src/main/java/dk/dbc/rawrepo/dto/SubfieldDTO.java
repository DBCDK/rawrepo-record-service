package dk.dbc.rawrepo.dto;

public class SubfieldDTO {

    private String name;
    private String value;

    public SubfieldDTO() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "SubfieldDTO{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
