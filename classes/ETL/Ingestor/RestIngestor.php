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
use ETL\DataEndpoint\FileOnDisk;
use ETL\Configuration\EtlConfiguration;
use ETL\EtlOverseerOptions;
use ETL\DbModel\Query;
use ETL\aOptions;
use ETL\iAction;
use ETL\VariableStore;

use PDO;
use Exception;
use Psr\Log\LoggerInterface;
use ZipArchive;

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

    // Optional POST request fields for the rest call
    protected $postFields = null;

    // Optional string version of $postFields to subtitute variables from source query and send as curl POST request
    protected $postFieldString = null;

    // Optional key to wrap POST fields for the rest call (NIH wraps their post fields with "criteria")
    protected $postFieldKey = null;

    // The current url, useful for debugging
    protected $currentUrl = null;

    // This action does not (yet) support multiple destination tables. If multiple destination
    // tables are present, store the first here and use it.
    protected $etlDestinationTable = null;

    // Rest results directory
    protected $restIngestDir = null;

    // Request records limit
    protected $limit = null;

    // Request records offset
    protected $offset = null;

    // Request records page
    protected $page = null;

    // If request is for a zip download
    protected $isZipDownload = false;

    // Current file name
    protected $filename = "";

    // Current file pointer
    protected $fp = null;

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

        if (isset($this->utilityEndpoint)) {
            if ( ! $this->utilityEndpoint instanceof aRdbmsEndpoint) {
                $this->logAndThrowException(
                    sprintf(
                        "Utility endpoint %s does not implement ETL\\DataEndpoint\\aRdbmsEndpoint",
                        get_class($this->utilityEndpoint)
                    )
                );
            }
        }

        // Would like to be able to say that this could support writing to database directly
        // but until we have database endpoints handle the insertion queries then they
        // are tightly coupled to the ingestors.
        //
        // Currently every ingestor is tightly coupled to just a database destination data endpoint type.
        // This is the only endpoint to my knowledge that is attempting to be agnostic to different
        // destination endpoints (disk, database). Other endpoints don't necessarily have such a use case
        // but there are good reasons to write database results to disk (databases change and its easier
        // to restore if we have the raw data on disk). To my understanding if the ETL framework aims to be modular
        // then we would really just have one ingestor that supports any source data endpoint and any destination
        // data endpoint.
        if ( ! $this->destinationEndpoint instanceof FileOnDisk ) {
            $this->logAndThrowException(
                sprintf(
                    "Destination endpoint %s does not implement ETL\\DataEndpoint\\FileOnDisk",
                    get_class($this->sourceEndpoint)
                )
            );
        }

        $this->sourceEndpoint->connect();

        $this->restIngestDir = $this->destinationEndpoint->getPath();
        $this->isZipDownload = $this->destinationEndpoint->getFileExtension() === 'zip';

        $this->limit = $this->sourceEndpoint->getLimit();
        $this->offset = $this->sourceEndpoint->getOffset();
        $this->page = $this->sourceEndpoint->page();
        // If the source query is specified in the definition file use it to obtain parameters for the
        // rest call. For each record returned by the source query, add the returned columnms to the
        // parameter list and generate one rest call. THIS OVERRIDES THE NEXT/PREV KEYS IN THE RESPONSE!

        if ( null === $this->etlSourceQuery && isset($this->parsedDefinitionFile->source_query) ) {
            $this->logger->debug("Create ETL source query object");
            $this->utilityEndpoint->connect();
            $this->etlSourceQuery = new Query(
                $this->parsedDefinitionFile->source_query,
                $this->utilityEndpoint->getSystemQuoteChar(),
                $this->logger
            );

            // If supported by the source query, set the date ranges here.

            $this->getEtlOverseerOptions()->applyOverseerRestrictions($this->etlSourceQuery, $this->utilityEndpoint, $this);

        }  // if ( null === $this->etlSourceQuery && isset($this->parsedDefinitionFile->source_query) )

        // Set up some default values for the REST response config. These can be overriden.

        if ( null !== $this->restRequestConfig && ! is_object($this->restRequestConfig) ) {
            $this->logAndThrowException("REST request config must be an object");
        } elseif ( null !== $this->restResponseConfig && ! is_object($this->restResponseConfig) ) {
            $this->logAndThrowException("REST response config must be an object");
        }

        // Rest response
        if ( null === $this->restResponseConfig && isset($this->parsedDefinitionFile->rest_response) ) {
            $this->restResponseConfig = (object) $this->parsedDefinitionFile->rest_response;
        } elseif ( ! isset($this->parsedDefinitionFile->rest_response) ) {
            $this->logAndThrowException("rest_response key not found in definition file");
        }

        // Rest Request
        if ( null === $this->restRequestConfig && isset($this->parsedDefinitionFile->rest_request) ) {
            $this->restRequestConfig = $this->parsedDefinitionFile->rest_request;
        } elseif ( ! isset($this->parsedDefinitionFile->rest_response) ) {
            $this->logAndThrowException("rest_request key not found in definition file");
        }

        if (isset($this->restRequestConfig->post_fields) ) {
            $this->postFieldString = json_encode($this->restRequestConfig->post_fields);
            if (false === $this->postFieldString) {
                $this->logAndThrowException("Could not JSON encode post field options within definition file");
            }

            curl_setopt($this->sourceHandle, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($this->sourceHandle, CURLOPT_POST, true);
            $this->postFields = [];
        }

        $this->postFieldKey = isset($this->restRequestConfig->post_field_key) ? $this->restRequestConfig->post_field_key : null;

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
        $responseKey = ( isset($this->restResponseConfig->response) ? $this->restResponseConfig->response : null );

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

        $this->setRestParameters();

        // Zip files need file setup beforehand to stream data from CURL
        if ($this->isZipDownload) {
            $this->destinationEndpoint->connect();
            curl_setopt($this->sourceHandle, CURLOPT_FILE, $this->destinationEndpoint->getHandle());
        }

        // Keep the current url for logging
        $this->currentUrl = curl_getinfo($this->sourceHandle, CURLINFO_EFFECTIVE_URL);

        curl_setopt($this->sourceHandle, CURLOPT_HTTPHEADER, $requestHeaders);

        $this->logger->info("REST url: {$this->currentUrl}");

        if ( $this->getEtlOverseerOptions()->isDryrun() ) {
            return 0;
        }

        $numRequestsMade = 1;
        $logCount = 10000;

        while ( false !== ( $retval = curl_exec($this->sourceHandle) ) ) {

            if ( 0 !== curl_errno($this->sourceHandle) ) {
                $this->logAndThrowException("${this} Error during REST call: " . curl_error($this->sourceHandle));

                if ($this->isZipDownload) {
                    $this->destinationEndpoint->disconnect();
                }
            }

            $numRequestsMade++;

            if ($this->isZipDownload) {
                $this->destinationEndpoint->unzipFile();

                if ( false === $this->setNextUrl($response, $nextKey) ) {
                    break;
                }
                continue;
            }

            $response = json_decode($retval);

            if ( null === $response || ! is_object($response) ) {
                $this->logAndThrowException("{$this} Response is not an object: $retval");
            }

            // --------------------------------------------------------------------------------
            // Identify the various parts of the response based on the configuration and verify them

            // If a top level response key is provided, grab the data that it contains.

            $results = null;
            if ( $responseKey !== null ) {
                if ( ! isset($response->$responseKey) ) {
                    $this->logger->warning(
                        "Configured top-level response key '$responseKey' not found in response. "
                        . "Response keys are '" . implode(",", array_keys((array) $response)) . "'"
                    );
                } else {
                    $results = $response->$responseKey;
                }
            } else {
                $results = $response;
            }

            if ( empty($results) ) {
                $this->logger->notice("Request returned an empty result set, skipping. url = {$this->currentUrl}");

                if ( false === $this->setNextUrl($response, $nextKey) ) {
                    break;
                }
                continue;
            }  // else ( 0 == count($results) )

            $this->destinationEndpoint->connect();
            $this->destinationEndpoint->writeDataToJSON($results);

            // Set up the next url using the "next" key or the source query values
            $moveOffset = isset($this->limit) && ( count(get_object_vars($results)) > $this->limit );
            if ( false === $this->setNextUrl($response, $nextKey, $moveOffset) ) {
                break;
            }

            if ( null !== $this->sourceEndpoint->getSleepMicroseconds() ) {
                usleep($this->sourceEndpoint->getSleepMicroseconds());
            }
        }  // while ( false !== ( $retval = curl_exec($this->sourceHandle) ) )

        if ( 0 != curl_errno($this->sourceHandle) ) {
            $this->logAndThrowException(curl_error($this->sourceHandle));
        }

        $this->logger->info("Made $numRequestsMade REST requests");

        return $numRequestsMade;
    }  // _execute()

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

        if (isset($this->restRequestConfig->parameters) ) {
            foreach( $this->restRequestConfig->parameters as $parameter => $value ) {

                $value = $this->variableStore->substitute($value);
                $this->setParameter($parameter, $value);
                $numParameters++;

            }  // foreach( $this->restRequestConfig->parameters as $parameter => $value )
        }

        if (isset($this->limit)) {
            $this->setParameter('limit', $this->limit);
        }

        if (isset($this->offset)) {
            $this->setParameter('offset', $this->offset);
        }

        if (isset($this->page)) {
            $this->setParameter('page', $this->page);
        }

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

        if (isset($this->postFields) ) {
            $this->postFields[$parameter] = $value;
        } else {
            $this->restParameters[$parameter] = $value;
        }
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

    protected function setRestParameters()
    {
        if ( 0 === count($this->restParameters) || 0 === count($this->postFields)) {
            return;
        }

        if (isset($this->postFields)) {
            $vs = new VariableStore($this->postFields);
            $postString = $vs->substitute($this->postFieldString);

            curl_setopt($this->sourceHandle, CURLOPT_POSTFIELDS, $postString);
            return $this;
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

    }  // setRestParameters()

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

    protected function setNextUrl(\stdClass $response, $nextKey, $moveOffset = false)
    {
        if (isset($this->limit)) {
            if (!isset($this->offset) && !isset($this->page)) {
                $this->logAndThrowException("Must set 'offset' or 'page' when using 'limit'.");
            }
        }
        // Check if we are using the same URL but making a new request due to offset/paginig
        //
        // If the next key was specified, use the value from the response for the next call. If we are
        // using a source query, do not use the next key returned in the response.
        if ($moveOffset) {
            if (isset($this->offset)) {
                $this->offset += $this->limit;
                $this->setParameter('offset', $this->offset);
                $this->logger->debug("New request record offset: {$this->offset}");
            } else {
                $this->page++;
                $this->setParameter('page', $this->page);
                $this->logger->debug("New request record page: {$this->page}");
            }
            $this->setRestParameters();
        } elseif ( null !== $this->etlSourceQuery && null !== $this->etlSourceQueryResult ) {

            if (isset($this->limit)) {
                if (isset($this->offset)) {
                    $this->offset = 0;
                    $this->setParameter('offset', $this->offset);
                } else {
                    $this->page = 0;
                    $this->setParameter('page', $this->offset);
                }
            }
            // Continue pulling from the source query until we reach the end or we pass parameter verification

            $row = false;

            while ( false !== ($row = $this->etlSourceQueryResult->fetch(PDO::FETCH_ASSOC)) ) {

                foreach( $row as $k => $v ) {
                    $this->setParameter($k, $v);
                }

                // Need to be able to skip this not end the run.

                if ( false !== $this->setRestParameters() ) {
                    // Zip files need file setup beforehand to stream data from CURL
                    if ($this->isZipDownload) {
                        $this->destinationEndpoint->connect();
                        curl_setopt($this->sourceHandle, CURLOPT_FILE, $this->destinationEndpoint->getHandle());
                    }
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

        if ( null !== $this->sourceEndpoint->getSleepMicroseconds() ) {
            usleep($this->sourceEndpoint->getSleepMicroseconds());
        }

        $this->logger->debug("REST url: {$this->currentUrl}");

        return true;

    }  // setNextUrl()
}  // class RestIngestor
