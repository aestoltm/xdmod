<?php
/**
 * FileOnDisk aims to suppport a 'destination' data endpoint for files. File writing is handled by ingestor.
 * The purpose of this class is to be explicit about source/destination endpoints during ingestion.
 * Similar conecpt to the directory scanner execpt the directory scanner is a 'source' data endpoint.
 */

namespace ETL\DataEndpoint;

use ETL\DataEndpoint\DataEndpointOptions;
use ETL\DataEndpoint\StructuredFile;
use Exception;
use Psr\Log\LoggerInterface;

class FileOnDisk extends aDataEndpoint
{
    /**
     * @const string Defines the name for this endpoint that should be used in configuration files.
     * It also allows us to implement auto-discovery.
     */

    const ENDPOINT_NAME = 'fileondisk';

    /**
     * @var string The directory path where files are written. This should be a fully qualified
     * path.
     */

    protected $path = null;

    public function __construct(DataEndpointOptions $options, LoggerInterface $logger = null)
    {
        parent::__construct($options, $logger);

        $requiredKeys = ['path'];
        $this->verifyRequiredConfigKeys($requiredKeys, $options);

        $this->path = $options->path;
        if ( 0 !== strpos($this->path, '/') ) {
            $this->logger->info(
                sprintf("%s: Relative path provided, absolute path recommended", $this)
            );
        }
    }

    /**
     * @return string The directory path that we will be scanning.
     */

    public function getPath()
    {
        return $this->path;
    }
}
