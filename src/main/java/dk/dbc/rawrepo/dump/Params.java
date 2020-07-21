package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.dto.ParamsValidationItemDTO;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class Params {

    String outputEncoding;
    String outputFormat;
    String mode;

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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    List<ParamsValidationItemDTO> validateParams() {
        final List<ParamsValidationItemDTO> result = new ArrayList<>();

        if (this.outputFormat == null) {
            this.outputFormat = OutputFormat.LINE.toString(); // Set default value
        } else {
            try {
                OutputFormat.fromString(this.outputFormat);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("outputFormat", "The value '" + this.outputFormat + "' is not a valid value. Allowed values are: " + OutputFormat.validValues()));
            }
        }

        if (this.outputEncoding == null) {
            this.outputEncoding = "UTF-8"; // Set default value
        } else {
            if (!("DANMARC2".equalsIgnoreCase(this.outputEncoding) || Charset.availableCharsets().containsKey(this.outputEncoding))) {
                result.add(new ParamsValidationItemDTO("outputEncoding", "The value '" + this.outputEncoding + "' is not a valid charset"));
            }
        }

        if (this.mode == null) {
            this.mode = Mode.MERGED.toString();
        } else {
            try {
                Mode.fromString(this.mode);
            } catch (IllegalArgumentException e) {
                result.add(new ParamsValidationItemDTO("mode", "The value '" + this.mode + "' is not a valid value. Allowed values are: " + Mode.validValues()));
            }
        }

        return result;
    }

}
