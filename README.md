<img src="http://www.dbc.dk/logo.png" alt="DBC" title="DBC" align="right">

# rawrepo-record-service
REST service for getting records from raw repo

## Endpoints
This service offers the following endpoints:

### Record data
    /api/v1/record/{agencyid}/{bibliographicrecordid}

This endpoint returns a JSON document with all data from the correspoding record row. Content is present in both JSON 
format (contentJSON field) and as byte encoded MarcXchange XML.

### Record content
    /api/v1/record/{agencyid}/{bibliographicrecordid}/content

This returns the content of the record as MarcXchange XML.

### Record list
    /api/v1/records/{agencyid}/{bibliographicrecordid}

This endpoint return a JSON document containing a list of all records which the input record depends on. The format for each record is the same as the /record endpoint

### Record content list
    /api/v1/records/{agencyid}/{bibliographicrecordid}/content

This endpoint return a MarcXchange XML collection of all record which the input record depends on.

## URL params
The endpoints make use of the following parameters:

### Mode
Mode is used to determine whether the content should be raw, merged or expanded. Mode is applicable to .

    Param name: 'mode'
    Applicable for: Both /record endpoints
    Valid values: 'raw', 'merged', 'expanded'.
    Default: 'raw' for record data endpoint, 'merged' for record content endpoint

### Allow deleted
The allow-deleted param is used to specify whether a record should be returned in case the record is deleted.

    Param name: 'allow-deleted'
    Applicable for: All endpoints
    Valid values: 'true', 'false'
    Default: 'false'

### Exclude DBC fields
If the letter fields (xYY where x is a letter and YY are numbers) should not be returned the exclude-dbc-fields param should be set to true.

    Param name: 'exclude-dbc-fields'
    Applicable for: All endpoints
    Valid values: 'true', 'false'
    Default: 'false'

### Expand
Used by the record content endpoints to define if the content should be expanded in addition to merged.

    Param name: 'expand'
    Applicable for: Both /records endpoints  
    Valid values: 'true', 'false'
    Default: 'false'

### Keep authority fields
In case subfields *5 and *6 should not be removed while expanding a record the keep-aut-fields param can be set to true.

    Param name: 'keep-aut-fields'
    Applicable for: All endpoints
    Valid values: 'true', 'false'
    Default: 'false'

### Use parent agency
All DBC enrichments have the same agency regardless the type of record they belong to (e.g. 870970, 870971 or 870979) while make it impossible to determine the type of record only by looking at the enrichment. A way around this is to find the record using 191919 but then overwrite the agency with the parent record. 

Setting use-parent-agency to true returns record with the parent agency - not that this should ONLY be used for DBC enrichments, never for FBS enrichments!

    Param name: 'use-parent-agency'
    Applicable for: All endpoints
    Valid values: 'true', 'false'
    Default: 'false'

## Examples
Single record with merged content:

    /api/v1/record/{agencyid}/{bibliographicrecordid}?mode=merged

Single record content, expanded:

    /api/v1/record/{agencyid}/{bibliographicrecordid}/content?mode=expanded

Record collection, expanded, keeping authority fields and replacing 191919 with the parent agency:

    /api/v1/records/{agencyid}/{bibliographicrecordid}/content?expand=true&keep-aut-fields=true&use-parent-agency=true