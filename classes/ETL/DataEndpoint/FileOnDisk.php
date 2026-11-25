<?php
/**
 * FileOnDisk aims to suppport a 'destination' data endpoint for files.
 * The purpose of this class is to be explicit about source/destination endpoints during ingestion.
 * Similar conecpt to the directory scanner execpt the directory scanner is a 'source' data endpoint.
 */

namespace ETL\DataEndpoint;

use ETL\DataEndpoint\DataEndpointOptions;
use ETL\DataEndpoint\StructuredFile;
use Exception;
use Psr\Log\LoggerInterface;
use ZipArchive;

class FileOnDisk extends File implements iDataEndpoint
{
    /**
     * @const string Defines the name for this endpoint that should be used in configuration files.
     * It also allows us to implement auto-discovery.
     */

    const ENDPOINT_NAME = 'fileondisk';

    /**
     * @var string The file exentsion (JSON, CSV, XML). Only supports JSON writing for now.
     */
    protected $fileExtension = null;

    /**
     * @var string Current temp file name
     */
    protected $tempFileName = null;

    /**
     * @see iDataEndpoint::__construct()
     */

    public function __construct(DataEndpointOptions $options, LoggerInterface $logger = null)
    {
        parent::__construct($options, $logger);

        $requiredKeys = ['path', 'file_extension'];
        $this->verifyRequiredConfigKeys($requiredKeys, $options);

        $messages = [];
        $propertyTypes = [
            'path' => 'string',
            'file_extension' => 'string',
        ];

        if ( ! \xd_utilities\verify_object_property_types($options, $propertyTypes, $messages, true) ) {
            $this->logAndThrowException("Error verifying options: " . implode(", ", $messages));
        }

        $this->path = $options->path;
        $this->fileExtension = $options->file_extension;
    }

    /**
     * @return string The directory path that we will be scanning.
     */

    public function getFileExtension()
    {
        return $this->fileExtension;
    }

    /* ------------------------------------------------------------------------------------------
     * Create temporary file and point handler to it. Supports zip files (used by RestIngestor)
     *
     * @return void
     *
     * @throw Exception If the file could not be created or opened
     * ------------------------------------------------------------------------------------------
     */

    public function connect() {
        if ( ($file = tempnam($this->path, 'xdmod-')) === false ) {
            $this->logAndThrowException("Could not create temp file, $file");
        }

        chmod($file, 0644);
        $tmpFile = $file . $this->fileExtension;
        rename($file, $tmpFile);

        $this->mode = $this->determineFileMode();
        if ( ($this->handler = fopen($tmpFile, $this->mode)) === false) {
            $this->logAndThrowException("Could not open $tmpFile");
        }

        $this->tmpFileName = $tmpFile;
    }

    /**
     * @return void Write php data (typically objects/arrays) to a JSON file.
     */

    public function writeDataToJSON(&$data)
    {
        if (fwrite($this->handler, json_encode($data)) === 0) {
            $this->logAndThrowException("Write failed to {$this->path}");
        }
    }

    /**
     * @return void Unzip a file and extract it to another location. After successful unzip clean up old file.
     */

    public function unzipFile()
    {
        if (!isset($this->tmpFileName) {
            $this->logAndThrowException("{$this} Missing file to unzip: {$this->tmpFileName}");
        }

        $this->disconnect();

        $zip = new ZipArchive;

        if (false === $zip->open($this->tmpFileName)) {
            $this->logAndThrowException("{$this} Error when opening zip file: {$this->tmpFileName}");
        }

        $extracted = $zip->extractTo($this->path);
        $zip->close();

        if (!$extracted) {
            $this->logAndThrowException("{$this} Error during unzip of {$this->tmpFileName}");
        }

        $this->logger->notice("{$this->tmpFileName} successfully unzipped.");
        unlink($this->tmpFileName)
        $this->tmpFileName = null;
    }

    /**
        * @return string Dynamically determine file write mode based on file extension.
        *                Put this in a seperate function to make easier to extend. If we
        *                want to force explicit file write mode then we can remove this.
     */

    public function determineFileMode()
    {
        return $this->fileExtension === 'zip' ? 'wb' : 'w';
    }

}
