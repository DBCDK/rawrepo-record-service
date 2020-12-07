/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.dto.ParamsValidationItemDTO;

import java.sql.Timestamp;
import java.util.List;

public class AgencyParams extends Params {

    private List<Integer> agencies;
    private String recordStatus;
    private List<String> recordType;
    String createdFrom;
    String createdTo;
    String modifiedFrom;
    String modifiedTo;

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

    public List<ParamsValidationItemDTO> validate(OpenAgencyServiceFromURL openAgencyServiceFromURL) {
        List<ParamsValidationItemDTO> result = validateParams();
        boolean hasFBSLibrary = false;

        if (this.agencies == null || this.agencies.isEmpty()) {
            result.add(new ParamsValidationItemDTO("agencies", "Field is mandatory and must contain at least one agency id"));
        } else {
            for (Integer agencyId : this.agencies) {
                if (agencyId.toString().length() != 6) {
                    result.add(new ParamsValidationItemDTO("agencies", "Agency " + agencyId.toString() + " is not a valid agency id as the text length is not 6 chars"));
                }

                if (agencyId == 191919 && this.agencies.size() > 1) {
                    result.add(new ParamsValidationItemDTO("agencies", "The combination of 191919 and other agencies is now allowed. If you are absolutely certain you want to dump agency 191919 (DBC enrichments) then it can be done with a request with 191919 as the only agency"));
                }
            }

            for (int agencyId : this.agencies) {
                try {
                    AgencyType agencyType = AgencyType.getAgencyType(openAgencyServiceFromURL, agencyId);

                    if (agencyType == AgencyType.FBS) {
                        hasFBSLibrary = true;
                    }
                } catch (OpenAgencyException e) {
                    result.add(new ParamsValidationItemDTO("agencies", "Agency " + agencyId + " could not be validated by OpenAgency"));
                }
            }
        }

        if (this.recordStatus == null) {
            this.recordStatus = RecordStatus.ACTIVE.toString(); // Set default value
        } else {
            try {
                RecordStatus.fromString(this.recordStatus);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("recordStatus", "The value " + this.recordStatus + " is not a valid value. Allowed values are: " + RecordStatus.validValues()));
            }
        }

        if (this.recordType != null) { // Validate values if present
            if (this.recordType.isEmpty()) {
                result.add(new ParamsValidationItemDTO("recordType", "If the field is present it must contain at least one value. Allowed values are: " + RecordType.validValues()));
            } else {
                for (String recordTypeString : this.recordType) {
                    try {
                        RecordType.fromString(recordTypeString);
                    } catch (IllegalArgumentException e) {
                        result.add(new ParamsValidationItemDTO("recordType", "The value " + recordTypeString + " is not a valid value. Allowed values are: " + RecordType.validValues()));
                    }
                }
            }
        } else { // Validate if recordType is allowed to not be present
            if (hasFBSLibrary) {
                result.add(new ParamsValidationItemDTO("recordType", "The field is required as agencies contains one or more FBS agencies"));
            }
        }

        if (this.createdFrom != null) {
            if (this.createdFrom.length() == 10) { // Add time to yyyy-dd-mm
                this.createdFrom = this.createdFrom + " 00:00:00";
            }

            try {
                Timestamp.valueOf(this.createdFrom);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("createdFrom", "The value '" + this.createdFrom + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        if (this.createdTo != null) {
            if (this.createdTo.length() == 10) { // Add time to yyyy-dd-mm
                this.createdTo = this.createdTo + " 23:59:59";
            }

            try {
                Timestamp.valueOf(this.createdTo);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("createdTo", "The value '" + this.createdTo + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        if (this.modifiedFrom != null) {
            if (this.modifiedFrom.length() == 10) { // Add time to yyyy-dd-mm
                this.modifiedFrom = this.modifiedFrom + " 00:00:00";
            }

            try {
                Timestamp.valueOf(this.modifiedFrom);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("modifiedFrom", "The value '" + this.modifiedFrom + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        if (this.modifiedTo != null) {
            if (this.modifiedTo.length() == 10) { // Add time to yyyy-dd-mm
                this.modifiedTo = this.modifiedTo + " 23:59:59";
            }

            try {
                Timestamp.valueOf(this.modifiedTo);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("modifiedTo", "The value '" + this.modifiedTo + "' doesn't have a valid format. Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]"));
            }
        }

        return result;
    }
}
