<?php
/* ==========================================================================================

 * REST service ingestor. Supports querying a REST endpoing and following previous/next urls in the
 * response and generatig requests based on the results of a query to the data warehouse.
 * Transformation and verificaiton of parameters and results is supported.
 *
 * @author Steve Gallo <smgallo@buffalo.edu>
 * @date 2016-02-05
 * ------------------------------------------------------------------------------------------
 */

namespace ETL\Ingestor;

use ETL\DataEndpoint\Rest;
use ETL\DataEndpoint\aRdbmsEndpoint;
use ETL\Configuration\EtlConfiguration;
use ETL\EtlOverseerOptions;
use ETL\DbModel\Query;
use ETL\aOptions;
use ETL\iAction;
use ETL\VariableStore;

use PDO;
use Exception;
use Psr\Log\LoggerInterface;

class RestIngestor extends aIngestor implements iAction
{
    // Parsed configuration options for REST request handling
    protected $restRequestConfig = null;

    // Parsed configuration options for REST response handling
    protected $restResponseConfig = null;

    // Column names for the destination table
    protected $destinationTableColumnNames = null;

    // Optional source query for additional parameters
    protected $etlSourceQuery = null;
    protected $etlSourceQueryResult = null;

    // Optional parameters for the rest call
    protected $restParameters = array();

    // The current url, useful for debugging
    protected $currentUrl = null;

    // This action does not (yet) support multiple destination tables. If multiple destination
    // tables are present, store the first here and use it.
    protected $etlDestinationTable = null;

    /* ------------------------------------------------------------------------------------------
     * Set up data endpoints and other options.
     *
     * @param IngestorOptions $options Options specific to this Ingestor
     * @param EtlConfiguration $etlConfig Parsed configuration options for this ETL
     * ------------------------------------------------------------------------------------------
     */

    public function __construct(aOptions $options, EtlConfiguration $etlConfig, LoggerInterface $logger = null)
    {
        parent::__construct($options, $etlConfig, $logger);

    }  // __construct()

    /* ------------------------------------------------------------------------------------------
     * @see iAction::initialize()
     * ------------------------------------------------------------------------------------------
     */

    public function initialize(EtlOverseerOptions $etlOverseerOptions = null)
    {
        if ( $this->isInitialized() ) {
            return;
        }

        $this->initialized = false;

        parent::initialize($etlOverseerOptions);

        if ( ! $this->sourceEndpoint instanceof Rest ) {
            $this->logAndThrowException(
                sprintf(
                    "Source endpoint %s does not implement ETL\\DataEndpoint\\Rest",
                    get_class($this->sourceEndpoint)
                )
            );
        }


        if ( ! $this->utilityEndpoint instanceof aRdbmsEndpoint ) {
            $this->logAndThrowException(
                sprintf(
                    "Utility endpoint %s endpoint is not an instance of ETL\\DataEndpoint\\aRdbmsEndpoint",
                    get_class($this->utilityEndpoint)
                )
            );
        }

        $this->sourceEndpoint->connect();
        $this->utilityEndpoint->connect();

        // If the source query is specified in the definition file use it to obtain parameters for the
        // rest call. For each record returned by the source query, add the returned columnms to the
        // parameter list and generate one rest call. THIS OVERRIDES THE NEXT/PREV KEYS IN THE RESPONSE!

        if ( null === $this->etlSourceQuery && isset($this->parsedDefinitionFile->source_query) ) {
            $this->logger->debug("Create ETL source query object");
            $this->etlSourceQuery = new Query(
                $this->parsedDefinitionFile->source_query,
                $this->utilityEndpoint->getSystemQuoteChar(),
                $this->logger
            );

            // If supported by the source query, set the date ranges here.

            $this->getEtlOverseerOptions()->applyOverseerRestrictions($this->etlSourceQuery, $this->utilityEndpoint, $this);

        }  // if ( null === $this->etlSourceQuery && isset($this->parsedDefinitionFile->source_query) )

        // Set up some default values for the REST response config. These can be overriden.

        $defaultRestResponseConfig = (object) array(
            // Optional top-level entry point into the result. NSF api uses "response".
            "response" => null,
        );

        if ( null !== $this->restRequestConfig && ! is_object($this->restRequestConfig) ) {
            $this->logAndThrowException("REST request config must be an object");
        } elseif ( null !== $this->restResponseConfig && ! is_object($this->restResponseConfig) ) {
            $this->logAndThrowException("REST response config must be an object");
        }

        // Rest response
        if ( null === $this->restResponseConfig && isset($this->parsedDefinitionFile->rest_response) ) {
            $this->restResponseConfig = (object) array_merge(
                (array) $defaultRestResponseConfig,
                (array) $this->parsedDefinitionFile->rest_response
            );
        } elseif ( ! isset($this->parsedDefinitionFile->rest_response) ) {
            $this->logAndThrowException("rest_response key not found in definition file");
        }

        // Rest Request
        if ( null === $this->restRequestConfig && isset($this->parsedDefinitionFile->rest_request) ) {
            $this->restRequestConfig = $this->parsedDefinitionFile->rest_request;
        } elseif ( ! isset($this->parsedDefinitionFile->rest_response) ) {
            $this->logAndThrowException("rest_request key not found in definition file");
        }

        // --------------------------------------------------------------------------------
        // The values for the request parameter and result field map

        if ( isset($this->restRequestConfig->parameters) ) {
            foreach ( $this->restRequestConfig->parameters as $parameter => &$value ) {
                if ( ! is_object($value) ) {
                    continue;
                }
                if ( ! isset($value->value) ) {
                    $this->logger->warning("{$this} Parameter '$parameter' object does not specify a 'value' key, skipping");
                    continue;
                }
                $value = $value->value;
            }
            unset($value); // Sever the reference with the last element

        }  // if ( isset($this->restRequestConfig->parameters) )

        $this->initialized = true;

        return true;

    }  // initialize()

    /* ------------------------------------------------------------------------------------------
     * @see aAction::performPreExecuteTasks()
     * ------------------------------------------------------------------------------------------
     */

    protected function performPreExecuteTasks()
    {

        parent::performPreExecuteTasks();

        // If any custom SQL fragments for insertion were specified, use them.

        if ( isset($this->parsedDefinitionFile->custom_insert_values_components) ) {
            $this->customInsertValuesComponents = $this->parsedDefinitionFile->custom_insert_values_components;
            if ( ! is_object($this->customInsertValuesComponents) ) {
                $this->logAndThrowException(
                    sprintf(
                        "custom_insert_values_components must be an object, %s given",
                        gettype($this->customInsertValuesComponents)
                    )
                );
            }
        } else {
            $this->customInsertValuesComponents = new stdClass();
        }

        // If using a source query, execute it and prepare the result set

        if ( null !== $this->etlSourceQuery ) {

            $sql = $this->variableStore->substitute(
                $this->etlSourceQuery->getSql(),
                "Undefined macros found in SQL"
            );

            $this->logger->debug("REST source query:\n$sql");
            $this->etlSourceQueryResult = $this->utilityHandle->query($sql, array(), true);

            if ( 0 == $this->etlSourceQueryResult->rowCount() ) {
                $this->logger->warning("{$this} Source query return 0 rows, exiting");
                return false;
            }
        }  // if ( null !== $this->etlSourceQuery ) {

        // Apply any parameters that are defined

        $this->processParameters();

        return true;

    }  // performPreExecuteTasks()

    /* ------------------------------------------------------------------------------------------
     * @see aIngestor::_execute()
     * ------------------------------------------------------------------------------------------
     */

     // @codingStandardsIgnoreLine
    protected function _execute()
    {
        // Support a source query, mapping from the source to rest parameters, rest field map
        $requestHeaders = ( isset($this->restRequestConfig->requestHeaders) ? (array) $this->restRequestConfig->requestHeaders : [] );
        $responseKey = ( isset($this->restResponseConfig->reponse) ? $this->restResponseConfig->response : null );

        // --------------------------------------------------------------------------------
        // Perform a-priori verifications

        $timeStart = microtime(true);

        // If using a source query, set parameters for the current result.

        if ( null !== $this->etlSourceQuery && null !== $this->etlSourceQueryResult ) {
            $row = $this->etlSourceQueryResult->fetch(PDO::FETCH_ASSOC);
            foreach( $row as $k => $v ) {
                $this->setParameter($k, $v);
            }
        }

        // --------------------------------------------------------------------------------
        // Retrieve and process the REST results

        $this->setRestUrlWithParameters();

        // Keep the current url for logging
        $this->currentUrl = curl_getinfo($this->sourceHandle, CURLINFO_EFFECTIVE_URL);

        curl_setopt($this->sourceHandle, CURLOPT_HTTPHEADER, $requestHeaders);

        $this->logger->info("REST url: {$this->currentUrl}");

        if ( $this->getEtlOverseerOptions()->isDryrun() ) {
            return 0;
        }

        $numRequestsMade = 1;
        $logCount = 10000;
        $first = true;

        $numRecords = 0;
        $warnings = [];

        if(!$this->destinationHandle->beginTransaction()) {
            $this->logAndThrowException(
                "Could not start transaction. Skipping ingestion.",
                ['endpoint' => $this]
            );
        }

        // The custom_insert_values_components option is an object that allows us to specify a
        // subquery to use when inserting data rather than the raw source value. If the destination
        // column is present as a key in the object, use the subquery, otherwise use "?" as a
        // placeholder. Note that the raw value will be provided to the subquery and it should
        // contain a single "?" placeholder.
        //
        // NOTE: Null values will not overwrite non-null values in the database. This is done to
        // handle destinations that can be populated by multiple sources with varying levels of
        // detail.

        $customInsertValuesComponents = $this->customInsertValuesComponents;

        // The destination field map may specify that the same source field is mapped to multiple
        // destination fields and the order that the source record fields are returned may be
        // different from the order the fields were specified in the map. For each destination
        // table, maintain a mapping between the field position in the map (index) and the source
        // fields so we cam properly build the SQL parameter list in the proper order. At the same
        // time generate other data structures that will be needed later.

        $destinationFieldIdToSourceFieldMap = [];

        // Templates for source field values containing pre-determined values such as variables or
        // macros
        $sourceFieldToValueMapTemplate = [];

        // Scalar source fields that map to source fields
        $simpleSourceFields = [];

        // Complex source fields that must be evaluated by the source endpoint
        $complexSourceFields = [];

        // Variables or macros that will be substituted
        $variableSourceFields = [];

        try {
            while ( false !== ( $retval = curl_exec($this->sourceHandle) ) ) {

                if ( 0 !== curl_errno($this->sourceHandle) ) {
                    $this->logger->logAndThrowException("${this} Error during REST call: " . curl_error($this->sourceHandle));
                }

                $response = json_decode($retval, true);

                if ( null === $response || ! is_array($response) ) {
                    $this->logger->logAndThrowException("{$this} Response is not an array: $retval");
                }

                // --------------------------------------------------------------------------------
                // Identify the various parts of the response based on the configuration and verify them

                // If a top level response key is provided, grab the data that it contains.

                $results = null;
                if ( $responseKey !== null ) {
                    if ( ! isset($response[$responseKey]) ) {
                        $this->logAndThrowException(
                            "Configured top-level response key '$responseKey' not found in response. "
                            . "Response keys are '" . implode(",", array_keys((array) $response)) . "'"
                        );
                    } else {
                        $results = $response[$responseKey];
                    }
                } else {
                    $results = $response;
                }

                if ( ! is_array($results) ) {
                    $this->logAndThrowException("Request results is expected to be an array. Type returned was " . gettype($results));
                } elseif ( 0 == count($results) ) {
                    $this->logger->notice("Request returned an empty result set, skipping. url = {$this->currentUrl}");

                    if ( false === $this->setNextUrl($response, $nextKey) ) {
                        break;
                    }
                    continue;
                }  // else ( 0 == count($results) )

                // --------------------------------------------------------------------------------
                // Perform some validation on the first pass through the result set.

                if ( $first ) {

                    $first = false;

                    // On the first pass through, check the fields returned to be sure that they map to the
                    // destination table columns. If the field map is not provided assume that the field names
                    // are all keys in the response.

                    $firstRecord = $results[0];
                    $resultKeyNames = array_keys($firstRecord);

                    $this->parseDestinationFieldMap($resultKeyNames, $this->sourceEndpoint);

                    foreach ( $this->destinationFieldMappings as $etlTableKey => $destFieldToSourceFieldMap ) {

                        $destinationFields = array_keys($destFieldToSourceFieldMap);

                        // Create a mapping from the source fields to the all of the destination field indexes
                        // they correspond to. At the same time, split the source fileds into lists of simple
                        // and complex fields.

                        $simpleSourceFields[$etlTableKey] = [];
                        $complexSourceFields[$etlTableKey] = [];
                        $variableSourceFields[$etlTableKey] = [];
                        $destinationFieldIdToSourceFieldMap[$etlTableKey] = [];

                        $destinationFieldIdToSourceFieldMap[$etlTableKey] = [];

                        foreach ( array_values($destFieldToSourceFieldMap) as $index => $sourceField ){
                            $destinationFieldIdToSourceFieldMap[$etlTableKey][$index] = $sourceField;
                            if (
                                $this->sourceEndpoint->supportsComplexDataRecords()
                                && $this->sourceEndpoint->isComplexSourceField($sourceField)
                            ) {
                                $complexSourceFields[$etlTableKey][] = $sourceField;
                            } elseif ( Utilities::containsVariable($sourceField) ) {
                                $variableSourceFields[$etlTableKey][] = $sourceField;
                            } else {
                                $simpleSourceFields[$etlTableKey][] = $sourceField;
                            }
                        }

                        $valuesComponents = array_map(
                            function ($destField) use ($customInsertValuesComponents) {
                                return ( property_exists($customInsertValuesComponents, $destField)
                                         ? $customInsertValuesComponents->$destField
                                         : '?' );
                            },
                            $destinationFields
                        );

                        $destinationFields = $this->quoteIdentifierNames($destinationFields);

                        $sql = sprintf(
                            'INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s',
                            $this->etlDestinationTableList[$etlTableKey]->getFullName(),
                            implode(', ', $destinationFields),
                            implode(', ', $valuesComponents),
                            implode(', ', array_map(
                                function ($destField) {
                                    return "$destField = COALESCE(VALUES($destField), $destField)";
                                },
                                $destinationFields
                            ))
                        );

                        try {
                            $this->logger->debug(
                                sprintf("Insert SQL for table key '%s':\n%s", $etlTableKey, $sql)
                            );
                            if ( ! $this->getEtlOverseerOptions()->isDryrun() ) {
                                $insertStatements[$etlTableKey] = $this->destinationHandle->prepare($sql);
                            }
                        } catch (PDOException $e) {
                            $this->logAndThrowException(
                                "Error preparing insert statement for table key '$etlTableKey'",
                                array('exception' => $e, 'endpoint' => $this)
                            );
                        }

                        // If there are source fields that are variables or macros, evaluate them once here and
                        // save them to a reusable template.

                        $sourceFieldToValueMapTemplate[$etlTableKey] = [];

                        if ( 0 != count($variableSourceFields[$etlTableKey]) ) {
                            foreach ( $variableSourceFields[$etlTableKey] as $variable ) {
                                $sourceFieldToValueMapTemplate[$etlTableKey][$variable] =
                                    $this->variableStore->substitute($variable);
                            }
                        }
                    } // foreach ( $this->destinationFieldMappings as $etlTableKey => $destFieldToSourceFieldMap )
                }  // if ( $first )

                if ( $this->getEtlOverseerOptions()->isDryrun() ) {
                    return $numRecords;
                }

                // When the destination field map is auto-generated, only scalar source fields are used. If
                // the source data is complex (e.g., JSON) we may end up with some complex fields in the
                // source record (e.g., JSON objects as stdClass). Obviously, these cannot be used in the
                // SQL parameter list but checking each field of each source record will reduce ingest
                // performance.  Perform a on the first record to provide some sanity checking.

                $invalidSourceValues = [];

                foreach ( $this->destinationFieldMappings as $etlTableKey => $destFieldToSourceFieldMap ) {
                    $parameters = $this->generateParametersFromSourceRecord(
                        $firstRecord,
                        $destinationFieldIdToSourceFieldMap[$etlTableKey],
                        $sourceFieldToValueMapTemplate[$etlTableKey],
                        $simpleSourceFields[$etlTableKey],
                        $complexSourceFields[$etlTableKey]
                    );

                    // Verify the parameters are scalars

                    foreach ( $parameters as $index => $value ) {
                        if ( null !== $value && ! is_scalar($value) ) {
                            $sourceField = $destinationFieldIdToSourceFieldMap[$etlTableKey][$index];
                            $invalidSourceValues[$etlTableKey][$sourceField] = $value;
                        }
                    }
                }

                if ( 0 != count($invalidSourceValues) ) {
                    $this->logger->error(sprintf("First record:%s%s", PHP_EOL, print_r($firstRecord, true)));
                    $this->logAndThrowException(
                        sprintf(
                            "Source record contains non-scalar values that cannot be used as SQL params. %s",
                            implode('; ', array_map(
                                function ($table, $invalidValues) {
                                    return sprintf(
                                        "Table '%s': %s",
                                        $table,
                                        implode(', ', array_map(
                                            function ($k, $v) {
                                                return sprintf("field '%s' = %s", $k, gettype($v));
                                            },
                                            array_keys($invalidValues),
                                            $invalidValues
                                        ))
                                    );
                                },
                                array_keys($invalidSourceValues),
                                $invalidSourceValues
                            ))
                        )
                    );
                }

                // Process each result from Rest URL
                foreach ( $results as $result ) {

                    foreach ( $this->destinationFieldMappings as $etlTableKey => $destFieldToSourceFieldMap ) {

                        $parsedValues = [];

                        // Use the field map so that we can parse the values with their provided path ($resultKey)
                        foreach ( $destFieldToSourceFieldMap[$etlTableKey] as $destField => $resultKey ) {
                            $parsedValues[$destField] = $this->extractField($result, $resultKey);
                        }

                        $this->transform($parsedValues);

                        $parameters = $this->generateParametersFromSourceRecord(
                            $parsedValues,
                            $destinationFieldIdToSourceFieldMap[$etlTableKey],
                            $sourceFieldToValueMapTemplate[$etlTableKey],
                            $simpleSourceFields[$etlTableKey],
                            $complexSourceFields[$etlTableKey]
                        );

                        try {
                            $insertStatements[$etlTableKey]->execute($parameters);
                        } catch (PDOException $e) {
                            $this->logger->debug(print_r($result, true));
                            $this->logAndThrowException(
                                sprintf(
                                    "Error inserting data into table key '%s' for url: '%s' on record %s.",
                                    $etlTableKey,
                                    $this->currentUrl,
                                    $numRecords
                                ),
                                array('exception' => $e, 'endpoint' => $this)
                            );
                        }

                        $numRecords++;

                        $warning = $this->destinationHandle->query("SHOW WARNINGS");

                        if ( count($warning) > 0 ) {
                            $warnings = array_merge($warnings, $warning);
                        }
                    }
                }

                // Set up the next url using the "next" key or the source query values

                if ( false === $this->setNextUrl($response, $nextKey) ) {
                    break;
                }

                $numRequestsMade++;

            }  // while ( false !== ( $retval = curl_exec($this->sourceHandle) ) )
        } catch (Exception $e) {
            $this->destinationHandle->rollback();
            $this->logAndThrowException(
                "Error committing transaction. Rolling back transaction.",
                array('exception' => $e, 'endpoint' => $this)
            );
        }

        $this->destinationHandle->commit();

        foreach ( $warnings as $message) {
            $this->logSqlWarnings($message, $this->etlDestinationTableList[$table]->getFullName());
        }

        if ( 0 != curl_errno($this->sourceHandle) ) {
            $this->logAndThrowException(curl_error($this->sourceHandle));
        }

        $this->logger->info("Made $numRequestsMade REST requests");

        return $numRecordsProcessed;

    }  // _execute()

    /**
     * Build up a parameter list suitable for an SQL query. The parameters must be in the proper
     * order as expected by the field list of the query (this mapping information is stored in
     * $destinationFieldIdToSourceFieldMap). Note that the same source value may be used multiple
     * times in the query.
     *
     * @param $sourceRecord The current record from the source endpoint (must be Traversable but
     *   may not explicitly implement Traversable such as an array or stdClass)
     * @param array $destinationFieldIdToSourceFieldMap A mapping between the parameter position
     *   (index) in the SQL statement and the source fields so we cam properly build the SQL
     *   parameter list in the correct order.
     * @param array $sourceTemplate Templates for source field values containing pre-determined
     *   values such as variables or macros.
     * @param array $simpleSourceFields Scalar source fields that map to source fields.
     * @param array $complexSourceFields Complex source fields that must be evaluated by the source
     *   endpoint
     *
     * @return array A list of values to use as SQL parameters in the proper order corresponding
     *   to the SQL query parameters.
     */

    private function generateParametersFromSourceRecord(
        $sourceRecord,
        array $destinationFieldIdToSourceFieldMap,
        array $sourceTemplate,
        array $simpleSourceFields,
        array $complexSourceFields
    ) {
        $sourceFieldToValueMap = $sourceTemplate;

        // Build up the parameter list for the query. Note that the same source value may be
        // used multiple times.

        foreach ($sourceRecord as $sourceField => $sourceValue) {
            if ( in_array($sourceField, $simpleSourceFields) ) {
                $sourceFieldToValueMap[$sourceField] = $sourceValue;
            }
        }

        // If this source endpoint does not support complex fields this loop won't be
        // processed because no fields will have been identified as complex.

        foreach ( $complexSourceFields as $sourceField ) {
            $sourceFieldToValueMap[$sourceField] =
                $this->sourceEndpoint->evaluateComplexSourceField($sourceField, $sourceRecord);
        }

        // Map the values from the source record to the correct order in the parameter list

        $parameters = array();
        foreach ( $destinationFieldIdToSourceFieldMap as $index => $sourceField ) {
            $parameters[$index] = $sourceFieldToValueMap[$sourceField];
        }

        return $parameters;

    }  // generateParametersFromSourceRecord()

    /* ------------------------------------------------------------------------------------------
     * The REST ingestor supports request parameters specified in the definition file. Process these
     * parameters, including any macros and add them to the parameter list.
     *
     * @return The number of parameters processed
     *
     * @throw Exception If a parameter was not formatted correctly
     * ------------------------------------------------------------------------------------------
     */

    protected function processParameters()
    {
        $numParameters = 0;

        if ( null === $this->restRequestConfig ||
             ! isset($this->restRequestConfig->parameters) )
        {
            return $numParameters;
        }

        foreach( $this->restRequestConfig->parameters as $parameter => $value ) {

            $value = $this->variableStore->substitute($value);
            $this->setParameter($parameter, $value);
            $numParameters++;

        }  // foreach( $this->restRequestConfig->parameters as $parameter => $value )

        return $numParameters;

    }  // processParameters()

    /* ------------------------------------------------------------------------------------------
     * Set an individual rest parameter.
     *
     * @param $parameter The parameter name
     * @param $value The parameter value
     *
     * @return This object for method chaining.
     * ------------------------------------------------------------------------------------------
     */

    protected function setParameter($parameter, $value)
    {
        if ( null === $parameter || empty($parameter) ) {
            $this->logAndThrowException("REST parameter name not provided");
        }

        $this->restParameters[$parameter] = $value;
        return $this;

    }  // setParameter()

    /* ------------------------------------------------------------------------------------------
     * Format request parameters and add them to the base url.  The format can be a standard querys
     * tring format or a format can be specified in the configuration. Macro substitution is supported
     * in both the parameters and the format.  A special ${REMAINING} macro is supported using the
     * format string that will evaluate to a query string containing any macros that were not used in
     * the format.
     *
     * @return This object for method chaining.
     * ------------------------------------------------------------------------------------------
     */

    protected function setRestUrlWithParameters()
    {
        if ( 0 == count($this->restParameters) ) {
            return;
        }

        if ( null !== $this->restRequestConfig && isset($this->restRequestConfig->format) ) {

            // A format was specified. Substitute any existing parameters in the format string.

            $substitutionDetails = array();
            $vs = new VariableStore($this->restParameters);
            $queryString = $vs->substitute($this->restRequestConfig->format, null, $substitutionDetails);

            if ( false !== strpos($queryString, '${^REMAINING}') ) {
                $used = array_combine($substitutionDetails['substituted'], $substitutionDetails['substituted']);
                $remaining = array_diff_key($this->restParameters, $used);
                $parameters = implode(
                    "&",
                    array_map(
                        function ($v, $k) {
                            return $k . "=" . urlencode($v);
                        },
                        $remaining,
                        array_keys($remaining)
                    )
                );
                $vs->clear();
                $vs->add('^REMAINING', $parameters);
                $queryString = $vs->substitute($queryString);
            }
        } else {
            // Use standard query string format

            $parameters = array_map(
                function ($v, $k) {
                    return $k . "=" . urlencode($v);
                },
                $this->restParameters,
                array_keys($this->restParameters)
            );
            $queryString = "?" . implode("&", $parameters);
        }

        $this->currentUrl = $newUrl = $this->sourceEndpoint->getBaseUrl() . $queryString;
        curl_setopt($this->sourceHandle, CURLOPT_URL, $newUrl);

        return $this;

    }  // setRestUrlWithParameters()

    /* ------------------------------------------------------------------------------------------
     * Set up the url for the next record.
     *
     * @param $response The REST response, used to check for a "next" key that will tell us the url
     *   for the next set of results
     * @param $nextKey The name of the "next" key
     *
     * @return true on success, false if there are no more records to fetch
     * ------------------------------------------------------------------------------------------
     */

    protected function setNextUrl(\stdClass $response, $nextKey)
    {
        // If the next key was specified, use the value from the response for the next call. If we are
        // using a source query, do not use the next key returned in the response.

        if ( null !== $this->etlSourceQuery && null !== $this->etlSourceQueryResult ) {

            // Continue pulling from the source query until we reach the end or we pass parameter verification

            $row = false;

            while ( false !== ($row = $this->etlSourceQueryResult->fetch(PDO::FETCH_ASSOC)) ) {


                foreach( $row as $k => $v ) {
                    $this->setParameter($k, $v);
                }

                // Need to be able to skip this not end the run.

                if ( false !== $this->setRestUrlWithParameters() ) {
                    break;
                }
            }
            if ( false === $row ) {
                return false;
            }

        } elseif ( null !== $nextKey ) {
            if ( ! isset($response->$nextKey) || null === $response->$nextKey ) {
                $this->logger->warning("Next property '$nextKey' not present or has null value in response, finished.");
                return false;
            } else {
                $this->currentUrl = $response->$nextKey;
                curl_setopt($this->sourceHandle, CURLOPT_URL, $response->$nextKey);
            }
        } else {
            // No next key and no source query
            return false;
        }

        $this->logger->debug("REST url: {$this->currentUrl}");

        if ( null !== $this->sourceEndpoint->getSleepMicroseconds() ) {
            usleep($this->sourceEndpoint->getSleepMicroseconds());
        }

        return true;

    }  // setNextUrl()

     /* ------------------------------------------------------------------------------------------
      * Parses PHP obj/array and retrieves desired field.
      * @param object $data          PHP obj representing data.
      * @param array $path          Array of keys to parse as path to field.
      *
      * @return mixed
      * ------------------------------------------------------------------------------------------
      */
    protected function extractField($data, $path)
    {
        if (empty($path)) {
            $this->logger->warning("{$this->currentUrl} provided empty path to parse field.");
        }

        $current = $data;

        foreach ($path as $segment) {
            if (is_object($current) && property_exists($current, $segment)) {
                $current = $current->$segment;
            } elseif (is_array($current) && array_key_exists($segment, $current)) {
                $current = $current[$segment];
            } else {
                $this->logger->warning("{$this->currentUrl} cannot resolve $segment for path: $path");
                return null;
            }
        }

        return $current;
    }

    /* @see /ETL/Ingestor/pdoIngestor::transform() as this serves a similar purpose.
     * This function is expected to be overriden to provide expanded functionality
     * such as exploding values into multiple columns or multiple rows, or just
     * formatting a value
     */
    protected function transform(array $srcRecord) {
        return $srcRecord;
    }
}  // class RestIngestor
