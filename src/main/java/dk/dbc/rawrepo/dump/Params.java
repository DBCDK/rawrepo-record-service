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

    public List<String> validate(OpenAgencyServiceFromURL openAgencyServiceFromURL) {
        List<String> result = new ArrayList<>();
        boolean hasFBSLibrary = false;

        if (this.agencies == null || this.agencies.size() == 0) {
            result.add("agencies: Der skal være mindst ét biblioteksnummer angivet");
        }

        for (Integer agencyId : this.agencies) {
            if (agencyId.toString().length() != 6) {
                result.add("agencies: Bibliotek " + agencyId.toString() + " er ikke et gyldigt biblioteksnummer da tekststrengen ikke er 6 tegn langt");
            }

            if (agencyId == 191919 && this.agencies.size() > 1) {
                result.add("agencies: Kombination af 191919 og andre bibliotekter er ikke gyldig. Hvis du er *helt* sikker på at du vil dumpe 191919 (DBC påhængsposter) så skal det gøres en i kørsel med kun 191919 i agencies");
            }
        }

        for (int agencyId : this.agencies) {
            try {
                AgencyType agencyType = AgencyType.getAgencyType(openAgencyServiceFromURL, agencyId);

                if (agencyType == AgencyType.FBS) {
                    hasFBSLibrary = true;
                }
            } catch (OpenAgencyException e) {
                result.add("agencies: Biblioteksnummer '" + agencyId + "' blev ikke valideret af OpenAgency");
            }
        }

        if (this.recordStatus == null) {
            this.recordStatus = RecordStatus.ACTIVE.toString(); // Set default value
        } else {
            try {
                RecordStatus.fromString(this.recordStatus);
            } catch (IllegalArgumentException e) {
                result.add("recordStatus: Værdien '" + this.recordStatus + "' er ikke en gyldig værdi. Feltet skal have en af følgende værdier: " + RecordStatus.validValues());
            }
        }

        if (this.outputFormat == null) {
            this.outputFormat = OutputFormat.LINE.toString(); // Set default value
        } else {
            try {
                OutputFormat.fromString(this.outputFormat);
            } catch (IllegalArgumentException e) {
                result.add("outputFormat: Værdien '" + this.outputFormat + "' er ikke en gyldig værdi. Feltet skal have en af følgende værdier: " + OutputFormat.validValues());
            }
        }

        if (this.recordType != null) { // Validate values if present
            if (this.recordType.size() == 0) {
                result.add("recordType: Hvis feltet er med, så skal listen indeholde mindst én af følgende værdier: " + RecordType.validValues());
            } else {
                for (String recordType : this.recordType) {
                    try {
                        RecordType.fromString(recordType);
                    } catch (IllegalArgumentException e) {
                        result.add("recordType: Værdien '" + recordType + "' er ikke en gyldig værdi. Feltet skal have en af følgende værdier: " + RecordType.validValues());
                    }
                }
            }
        } else { // Validate if recordType is allowed to not be present
            if (hasFBSLibrary) {
                result.add("recordType: Da der findes mindst ét FBS bibliotek i agencies er dette felt påkrævet.");
            }
        }

        if (this.outputEncoding == null) {
            this.outputEncoding = "UTF-8"; // Set default value
        } else {
            try {
                Charset.forName(this.outputEncoding);
            } catch (UnsupportedCharsetException ex) {
                result.add("outputEncoding: Værdien '" + this.outputEncoding + "' er ikke et gyldigt charset");
            }
        }

        if (this.createdFrom != null) {
            try {
                Timestamp.valueOf(this.createdFrom);
            } catch (IllegalArgumentException e) {
                result.add("createdFrom: Værdien i 'createdFrom' har ikke et datoformat som kan fortolkes");
            }
        }

        if (this.createdTo != null) {
            try {
                Timestamp.valueOf(this.createdTo);
            } catch (IllegalArgumentException e) {
                result.add("createdTo: Værdien i 'createdTo' har ikke et datoformat som kan fortolkes");
            }
        }


        if (this.modifiedFrom != null) {
            try {
                Timestamp.valueOf(this.modifiedFrom);
            } catch (IllegalArgumentException e) {
                result.add("modifiedFrom: Værdien i 'modifiedFrom' har ikke et datoformat som kan fortolkes");
            }
        }

        if (this.modifiedTo != null) {
            try {
                Timestamp.valueOf(this.modifiedTo);
            } catch (IllegalArgumentException e) {
                result.add("modifiedTo: Værdien i 'modifiedTo' har ikke et datoformat som kan fortolkes");
            }
        }

        return result;
    }
}
