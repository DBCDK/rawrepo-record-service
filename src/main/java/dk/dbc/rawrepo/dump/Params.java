/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import java.util.List;

public class Params {

    private List<Integer> agencies;
    private String recordStatus;
    private List<String> recordType;
    private String createdFrom;
    private String createdTo;
    private String modifiedFrom;
    private String modifiedTo;
    private String outputEncoding;
    private String outputFormat;
    private int rowLimit;

    public int getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    public List<Integer> getAgencies() {
        return agencies;
    }

    public void setAgencies(List<Integer> agencies) {
        this.agencies = agencies;
    }

    public String getRecordStatus() {
        return recordStatus;
    }

    public void setRecordStatus(String recordStatus) {
        this.recordStatus = recordStatus;
    }

    public List<String> getRecordType() {
        return recordType;
    }

    public void setRecordType(List<String> recordType) {
        this.recordType = recordType;
    }

    public String getCreatedFrom() {
        return createdFrom;
    }

    public void setCreatedFrom(String createdFrom) {
        this.createdFrom = createdFrom;
    }

    public String getCreatedTo() {
        return createdTo;
    }

    public void setCreatedTo(String createdTo) {
        this.createdTo = createdTo;
    }

    public String getModifiedFrom() {
        return modifiedFrom;
    }

    public void setModifiedFrom(String modifiedFrom) {
        this.modifiedFrom = modifiedFrom;
    }

    public String getModifiedTo() {
        return modifiedTo;
    }

    public void setModifiedTo(String modifiedTo) {
        this.modifiedTo = modifiedTo;
    }

    public String getOutputEncoding() {
        return outputEncoding;
    }

    public void setOutputEncoding(String outputEncoding) {
        this.outputEncoding = outputEncoding;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public String toString() {
        return "Params{" +
                "agencies=" + agencies +
                ", recordStatus='" + recordStatus + '\'' +
                ", recordType=" + recordType +
                ", createdFrom='" + createdFrom + '\'' +
                ", createdTo='" + createdTo + '\'' +
                ", modifiedFrom='" + modifiedFrom + '\'' +
                ", modifiedTo='" + modifiedTo + '\'' +
                ", outputEncoding='" + outputEncoding + '\'' +
                ", outputFormat='" + outputFormat + '\'' +
                ", rowLimit=" + rowLimit +
                '}';
    }
}
