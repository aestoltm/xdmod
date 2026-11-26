<?php
/**
 * CSV file data endpoint. Includes support for parsing a CSV file and returning the parsed
 * representation via the parse() method.
 */

namespace ETL\DataEndpoint;

use Exception;
use ETL\DataEndpoint\DataEndpointOptions;
use ETL\DataEndpoint\Filter\ExternalProcess;

use Psr\Log\LoggerInterface;

class CsvFile extends aStructuredFile implements iStructuredFile
{
    /**
     * @const string Defines the name for this endpoint that should be used in configuration files.
     * It also allows us to implement auto-discovery.
     */

    const ENDPOINT_NAME = 'csvfile';

    /**
     * @see iDataEndpoint::__construct()
     */

    public function __construct(DataEndpointOptions $options, LoggerInterface $logger = null)
    {
        parent::__construct($options, $logger);
    }

    /**
     * Decodes a CSV string into a PHP object and add it to the record list.
     *
     * @see aStructuredFile::decodeRecord()
     */

    protected function decodeRecord($data)
    {

        $decoded = @str_getcsv($data, ',', '"', '\\');

        if ( false === $decoded ) {
            $error = error_get_last();
            $this->logAndThrowException(
                sprintf(
                    "Error decoding CSV from file '%s': %s\n%s",
                    $this->path,
                    $error['message'],
                    $data
                )
            );
        }

        $this->recordList[] = $decoded;

        return true;
    }

    /**
     * @see aStructuredFile::verifyData()
     */

    protected function verifyData()
    {
        return true;
    }

    /**
     * @see aStructuredFile::discoverRecordFieldNames()
     */

    protected function discoverRecordFieldNames()
    {
        // If there are no records in the file then we don't need to set the discovered
        // field names.

        if ( 0 == count($this->recordList) ) {
            return;
        }

        // Determine the record names based on the structure of the JSON that we are
        // parsing.

        reset($this->recordList);
        $record = current($this->recordList);

        if ( is_array($record) ) {

            if ( $this->hasHeaderRecord ) {

                // If we have a header record skip the first record and use its values as
                // the field names

                $this->discoveredRecordFieldNames = array_shift($this->recordList);

            } elseif ( 0 !== count($this->requestedRecordFieldNames) ) {

                // If there is no header record and the requested field names have been
                // provided, use them as the discovered field names.  If a subsequent
                // record contains fewer fields return NULL values for those fields, if a
                // subsequent record contains more fields ignore them.

                $this->discoveredRecordFieldNames = $this->requestedRecordFieldNames;

            } else {
                $this->logAndThrowException("Record field names must be specified for JSON array records");
            }
        } else {
            $this->logAndThrowException(
                sprintf("Unsupported record type in %s. Got %s, expected array or object", $this->path, gettype($record))
            );
        }

        // If no field names were requested, return all discovered fields

        if ( 0 == count($this->requestedRecordFieldNames) ) {
            $this->requestedRecordFieldNames = $this->discoveredRecordFieldNames;
        }

    }

    /**
     * @see iStructuredFile::supportsComplexDataRecords()
     */

    public function supportsComplexDataRecords()
    {
        return false;
    }

    protected function transform($record){
        return $record;
    };
}
