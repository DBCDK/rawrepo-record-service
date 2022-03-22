<img src="http://www.dbc.dk/logo.png" alt="DBC" title="DBC" align="right">

# rawrepo-record-service
REST service for getting records from raw repo

## Endpoints
This service offers the following endpoints:

### Record data
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}

This endpoint returns a JSON document with all data from the correspoding record row. Content is present in both JSON 
format (contentJSON field) and as byte encoded MarcXchange XML.
$
Parameters:

    mode - raw | merged | expanded
    allow-deleted
    exclude-dbc-fields
    use-parent-agency
    keep-aut-fields
    exclude-attribute

### Record content
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/content

This returns the content of the record as MarcXchange XML.

Parameters:

    mode - merged | expanded
    allow-deleted
    exclude-dbc-fields
    use-parent-agency
    keep-aut-fields

### Record entry
    GET /api/v1/record-entries/{agencyid}/{bibliographicrecordid}/raw

This endpoint returns a JSON document with all data from the corresponding record row entry in its raw form. 

Content is presented as MarcJson.
    
### Record exists
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/exists

Returns a JSON document with a 'value' element which is true is the record is found - otherwise false is returned.

Note that by default this function will only look for active records. To also include deleted record the allow-deleted url param can be used.

Parameters:

    allow-deleted

### Record meta data
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/meta

Returns a JSON document with a record, but without the content fields.

Parameters:

    allow-deleted
    
### Record parents
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/parents

Returns a list of record ids of all parent records.

### Record children
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/children
    
Returns a list of record ids of all child records.

### Siblings from this record
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/siblings-from
    
Returns a list of record ids of all parent records with same bibliographicrecordid.

### Siblings to this record
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/siblings-to
    
Returns a list of record ids of all child records with same bibliographicrecordid.

### Relations form this record
    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/relations-from
    
Returns a list of record ids of all records which this record has relations to.

### Record history
    GET v1/record/{agencyid}/{bibliographicrecordid}/history
    
Returns a list of list meta data including modified date of all previous versions of the record.    

### Historic record
    GET v1/record/{agencyid}/{bibliographicrecordid}/{date}
    
Returns the raw record as it looked on {date}. Date must a 'modified' date from /history

### Records collection
    GET /api/v1/records/{agencyid}/{bibliographicrecordid}

This endpoint return a JSON document containing a list of all records which the input record depends on. The format for each record is the same as the /record endpoint.

Parameters:

    allow-deleted
    exclude-dbc-fields
    use-parent-agency
    expand
    keep-aut-fields
    exclude-attribute
    for-corepo
    
### Records content list
    GET /api/v1/records/{agencyid}/{bibliographicrecordid}/content

This endpoint returns a MarcXchange collection document containing all records which the input record depends on.

Parameters:
    
    allow-deleted
    exclude-dbc-fields
    use-parent-agency
    expand
    keep-aut-fields
    for-corepo

### Records bulk load
    POST /api/v1/records/bulk

This endpoint returns a JSON document with a collection of all records from the input. Input is a in the form of:

    {
        "recordIds": [
            {"bibliographicRecordId":"19000117","agencyId":191919},
            {"bibliographicRecordId":"19029174","agencyId":191919},
            {"bibliographicRecordId":"69998402","agencyId":191919}
        ]
    }

Currently there is no hard upper limit as for how many records can be loaded but the recommended limit is 200.

Parameters:
    
    allow-deleted
    exclude-dbc-fields
    use-parent-agency
    expand
    keep-aut-fields

### Agency list
    GET /api/v1/agencies
    
Returns a list of agencies which have at least one record.

### Agency record list
    GET /api/v1/agency/{agencyid}/recordids

Returns a list of ids on all records for the given agency. Note that the response object can be very large (> 100 MB)

Parameters:
    
    allow-deleted
    internal-agency-handling
    created-before
    created-after
    modified-before
    modified-after
    

## URL params
The endpoints make use of the following parameters:

### Mode
Mode is used to determine whether the content should be raw, merged or expanded. Mode is applicable to.

    Param name: 'mode'
    Valid values: 'raw', 'merged', 'expanded'.
    Default: 'raw' for record data endpoint, 'merged' for record content endpoint

### Allow deleted
The allow-deleted param is used to specify whether a record should be returned in case the record is deleted.

    Param name: 'allow-deleted'
    Valid values: 'true', 'false'
    Default: 'false'

### Exclude DBC fields
If the letter fields (xYY where x is a letter and YY are numbers) should not be returned the exclude-dbc-fields param should be set to true.

    Param name: 'exclude-dbc-fields'
    Valid values: 'true', 'false'
    Default: 'false'

### Expand
Used by the record content endpoints to define if the content should be expanded in addition to merged.

    Param name: 'expand'
    Valid values: 'true', 'false'
    Default: 'false'

### Keep authority fields
In case subfields *5 and *6 should not be removed while expanding a record the keep-aut-fields param can be set to true.

    Param name: 'keep-aut-fields'
    Valid values: 'true', 'false'
    Default: 'false'

### Use parent agency
All DBC enrichments have the same agency regardless the type of record they belong to (e.g. 870970, 870971 or 870979) while make it impossible to determine the type of record only by looking at the enrichment. A way around this is to find the record using 191919 but then overwrite the agency with the parent record. 

Setting use-parent-agency to true returns record with the parent agency - not that this should ONLY be used for DBC enrichments, never for FBS enrichments!

    Param name: 'use-parent-agency'
    Valid values: 'true', 'false'
    Default: 'false'

### For Corepo
This parameter is used for telling record service to find the most relevant active record. In the case of deleted enrichments it means the active common record is returned.
Setting for-corepo to true might mean the returned record is not from the specified agency!

Cases:
    
    Active common record and active enrichment -> Enrichment is returned
    Active common record and deleted enrichment -> Common record is returned
    Deleted common record and deleted enrichment -> The deleted enrichment is returned    

The use case for this parameter is when creating searchable volume records for corepo. If an enrichment agency has a deleted volume record but an active head record the corepo record has to be a combination of the common volume record and the enrichment head record
    
    Param name: 'for-corepo'
    Valid values: 'true', 'false'
    Default: 'false'

### Internal agency handling
This is used specifically for getting record id pairs with agency 191919 instead of a common agency. The param is used for agency record list. 
Without internal-agency-handling = true the response will contain sets of bibliographic record id and the input agency id.
When internal-agency-handling = true and the agency is one of the DBC common libraries (870970/870971/870979) the response will contain sets of bibliographic record id and agency 191919.

This parameter is used for getting sets of record ids which can be used for either getting merged common records or record ids for queuing.

    Param name: 'internal-agency-handling'
    Valid values: 'true', 'false'
    Default: 'false'

### Created before
This parameter is used to narrow the amount of returned records for an agency. This parameter can be used in combination with the other -before and -after parameters.

Note that the 'before' parameter selects 'up until' (exclusive) the defined date while 'after' selects from the beginning of the date (inclusive)

    Param-name: 'created-before'
    Valid values: YYYY-MM-DD
    Default: not set

### Created after
This parameter is used to narrow the amount of returned records for an agency. This parameter can be used in combination with the other -before and -after parameters.

Note that the 'before' parameter selects 'up until' (exclusive) the defined date while 'after' selects from the beginning of the date (inclusive)

    Param-name: 'created-after'
    Valid values: YYYY-MM-DD
    Default: not set

### Modified before
This parameter is used to narrow the amount of returned records for an agency. This parameter can be used in combination with the other -before and -after parameters.

Note that the 'before' parameter selects 'up until' (exclusive) the defined date while 'after' selects from the beginning of the date (inclusive)

    Param-name: 'modified-before'
    Valid values: YYYY-MM-DD
    Default: not set

### Modified after
This parameter is used to narrow the amount of returned records for an agency. This parameter can be used in combination with the other -before and -after parameters.

Note that the 'before' parameter selects 'up until' (exclusive) the defined date while 'after' selects from the beginning of the date (inclusive)

    Param-name: 'modified-after'
    Valid values: YYYY-MM-DD
    Default: not set

## Response codes
### 200 Ok
If the request was successful and data is returned the status code is 200.  

### 204 No Content
If the request is successful but no data is returned status code 204 is returned

### 404 Not found
If the request doesn't hit a valid end point 404 Not found is returned

### 500 Internal server error
If an unhandled exception occurs status code 500 is returned 
    

## Examples
##### Single record with merged content:

    GET /api/v1/record/{agencyid}/{bibliographicrecordid}?mode=merged

##### Single record content, expanded:

    GET /api/v1/record/{agencyid}/{bibliographicrecordid}/content?mode=expanded

##### Record collection, expanded, keeping authority fields and replacing 191919 with the parent agency:

    GET /api/v1/records/{agencyid}/{bibliographicrecordid}/content?expand=true&keep-aut-fields=true&use-parent-agency=true
    
##### All records for an agency modified since midnight:

    GET /api/v1/agency/{agencyid}/recordids?allow-deleted=false&modified-after={date}&internal-agency-handling=true
    
This will result in a JSON with a set of record ids, e.g.:

    {
        "recordIds": [
            {
                "bibliographicRecordId": "123456789",
                "agencyId": 191919
            },
            {
                "bibliographicRecordId": "123456790",
                "agencyId": 191919
            }
        ]
    }

##### Bulk set of data:

    POST /api/v1/records/bulk?use-parent-agency=true&expand=true&keep-aut-fields=true
    
Data is of the same format as the output from endpoint above.