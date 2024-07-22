<?php

namespace App\Helpers;

use Illuminate\Database\Connection;

class SyncContext
{
    public Connection $externalDb;
    public array $databaseData;
    public string $table;
    public string $databaseId;
    public string $schema;
    public string $uploadDbSchema;
    public string $type;
    public array $uploadDbData;
    public string $connectionNameUpload;

    public function __construct(
        Connection $externalDb,
        array $databaseData,
        string $table,
        string $databaseId,
        string $schema,
        string $uploadDbSchema,
        string $type,
        array $uploadDbData,
        string $connectionNameUpload
    ) {
        $this->externalDb = $externalDb;
        $this->databaseData = $databaseData;
        $this->table = $table;
        $this->databaseId = $databaseId;
        $this->schema = $schema;
        $this->uploadDbSchema = $uploadDbSchema;
        $this->type = $type;
        $this->uploadDbData = $uploadDbData;
        $this->connectionNameUpload = $connectionNameUpload;
    }
}

