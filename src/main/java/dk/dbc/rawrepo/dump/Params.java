/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Timestamp;
import java.util.ArrayList;
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
                '}';
    }

    public List<ParamsValidationItem> validate(OpenAgencyServiceFromURL openAgencyServiceFromURL) {
        List<ParamsValidationItem> result = new ArrayList<>();
        boolean hasFBSLibrary = false;

        if (this.agencies == null || this.agencies.size() == 0) {
            result.add(new ParamsValidationItem("agencies", "Field is mandatory and must contain at least one agency id"));
        }

        for (Integer agencyId : this.agencies) {
            if (agencyId.toString().length() != 6) {
                result.add(new ParamsValidationItem("agencies", "Agency " + agencyId.toString() + " is not a valid agency id as the text length is not 6 chars"));
            }

            if (agencyId == 191919 && this.agencies.size() > 1) {
                result.add(new ParamsValidationItem("agencies", "The combination of 191919 and other agencies is now allowed. If you are absolutely certain you want to dump agency 191919 (DBC enrichments) then it can be done with a request with 191919 as the only agency"));
            }
        }

        for (int agencyId : this.agencies) {
            try {
                AgencyType agencyType = AgencyType.getAgencyType(openAgencyServiceFromURL, agencyId);

                if (agencyType == AgencyType.FBS) {
                    hasFBSLibrary = true;
                }
            } catch (OpenAgencyException e) {
                result.add(new ParamsValidationItem("agencies", "Agency " + agencyId + " could not be validated by OpenAgency"));
            }
        }

        if (this.recordStatus == null) {
            this.recordStatus = RecordStatus.ACTIVE.toString(); // Set default value
        } else {
            try {
                RecordStatus.fromString(this.recordStatus);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItem("recordStatus", "The value " + this.recordStatus + " is not a valid value. Allowed values are: " + RecordStatus.validValues()));
            }
        }

        if (this.outputFormat == null) {
            this.outputFormat = OutputFormat.LINE.toString(); // Set default value
        } else {
            try {
                OutputFormat.fromString(this.outputFormat);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItem("outputFormat", "The value " + this.outputFormat + " is not a valid value. Allowed values are: " + OutputFormat.validValues()));
            }
        }

        if (this.recordType != null) { // Validate values if present
            if (this.recordType.size() == 0) {
                result.add(new ParamsValidationItem("recordType", "If the field is present it must contain at least one value. Allowed values are: " + RecordType.validValues()));
            } else {
                for (String recordType : this.recordType) {
                    try {
                        RecordType.fromString(recordType);
                    } catch (IllegalArgumentException e) {
                        result.add(new ParamsValidationItem("recordType", "The value " + recordType + " is not a valid value. Allowed values are: " + RecordType.validValues()));
                    }
                }
            }
        } else { // Validate if recordType is allowed to not be present
            if (hasFBSLibrary) {
                result.add(new ParamsValidationItem("recordType", "The field is required as agencies contains one or more FBS agencies"));
            }
        }

        if (this.outputEncoding == null) {
            this.outputEncoding = "UTF-8"; // Set default value
        } else {
            try {
                Charset.forName(this.outputEncoding);
            } catch (UnsupportedCharsetException ex) {
                result.add(new ParamsValidationItem("outputEncoding", "The value " + this.outputEncoding + " is not a valid charset"));
            }
        }

        if (this.createdFrom != null) {
            if (this.createdFrom.length() == 10) { // Add time to yyyy-dd-mm
                this.createdFrom = this.createdFrom + " 00:00:00";
            }

            try {
                Timestamp.valueOf(this.createdFrom);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItem("createdFrom", "The value '" + this.createdFrom + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        if (this.createdTo != null) {
            if (this.createdTo.length() == 10) { // Add time to yyyy-dd-mm
                this.createdTo = this.createdTo + " 23:59:59";
            }

            try {
                Timestamp.valueOf(this.createdTo);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItem("createdTo", "The value '" + this.createdTo + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }


        if (this.modifiedFrom != null) {
            if (this.modifiedFrom.length() == 10) { // Add time to yyyy-dd-mm
                this.modifiedFrom = this.modifiedFrom + " 00:00:00";
            }

            try {
                Timestamp.valueOf(this.modifiedFrom);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItem("modifiedFrom", "The value '" + this.modifiedFrom + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        if (this.modifiedTo != null) {
            if (this.modifiedTo.length() == 10) { // Add time to yyyy-dd-mm
                this.modifiedTo = this.modifiedTo + " 23:59:59";
            }

            try {
                Timestamp.valueOf(this.modifiedTo);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItem("modifiedTo", "The value '" + this.modifiedTo + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        return result;
    }
}
