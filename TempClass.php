<?php

namespace App\Jobs;

use App\Helpers\SyncContext;
use App\Models\SyncDb;
use App\Models\SyncSchedule;
use App\Models\SyncUpdate;
use Carbon\Carbon;
use Cron\CronExpression;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Table;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;
use Jsor\Doctrine\PostGIS\Schema\SchemaManager;

class SyncJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function handle(): void
    {
        $schedules = SyncSchedule::where('is_enabled', true)->get();

        foreach ($schedules as $schedule) {
            $this->processSchedule($schedule);
        }
    }

    protected function processSchedule(SyncSchedule $schedule): void
    {
        $cron = new CronExpression($schedule->cron);
        if (!$cron->isDue(Carbon::now())) {
            return;
        }

        $dbConfig = SyncDb::find($schedule->db);
        if (!$dbConfig) {
            Log::error("Конфигурация пуста, Schedule: {$schedule->id}");
            return;
        }

        $syncType = $schedule->sync;
        $connectionDetails = $this->getDataFromModel($dbConfig->data);
        $cacheKey = $this->generateCacheKey($connectionDetails);

        if (Cache::has($cacheKey)) {
            return;
        }

        $this->installExtensionsUploadDb($this->getDataFromModel($schedule->data));
        Cache::put($cacheKey, true, $this->getSyncDuration());
        $startTime = Carbon::now();

        try {
            DB::transaction(function () use ($syncType, $schedule, $dbConfig): void {
                switch ($syncType) {
                    case 'update':
                        $this->syncUpdate($dbConfig);
                        break;
                    case 'full':
                        $this->syncFull($dbConfig);
                        break;
                    case 'schema':
                        $this->syncSchema($dbConfig);
                        break;
                    case 'full_schema':
                        $this->syncFullSchema($dbConfig);
                        break;
                    default:
                        Log::warning("Неизвестный тип синхронизации {$syncType} для Schedule {$schedule->id}");
                }
            });

            $this->logSuccess($schedule, $startTime);
        } catch (\Exception $e) {
            Log::warning("Ошибка для Schedule {$schedule->id}: " . $e->getMessage());
        }
    }

    private function syncUpdate(SyncDb $database): void
    {
        $this->processTables($database, 'update');
    }

    private function syncFull(SyncDb $database): void
    {
        $this->processTables($database, 'full');
    }

    private function syncSchema(SyncDb $database): void
    {
        $downloadDbData = $this->getDataFromModel($database->data);
        $connectionName = $this->configureConnection($downloadDbData, $database->type);
        $externalDb = DB::connection($connectionName);
        $schemaManager = $externalDb->getDoctrineSchemaManager();
        $tables = $this->filterTables($schemaManager->listTableNames());

        $schema = $downloadDbData['schema'] ?? 'public';

        $uploadDb = SyncSchedule::where('db', $database->id)->first();
        $uploadDbData = $this->getDataFromModel($uploadDb->data);
        $uploadDbSchema = $uploadDbData['schema'] ?? 'public';
        $connectionNameUpload = $this->configureConnection($uploadDbData, Arr::get($uploadDbData, 'type'));

        foreach ($tables as $table) {
            $this->syncTableSchema($externalDb, $schemaManager, $table, $downloadDbData, $uploadDbData, $schema, $uploadDbSchema, $connectionNameUpload);
        }
    }

    private function syncTableSchema(Connection $externalDb, AbstractSchemaManager $schemaManager, string $table, array $downloadDbData, array $uploadDbData, string $schema, string $uploadDbSchema, string $connectionNameUpload): void
    {
        Log::debug("Начата синхронизация схемы из $schema." . $downloadDbData['database'] . " в $uploadDbSchema." . $uploadDbData['database']);

        try {
            $schemaTable = $schemaManager->introspectTable($table);
        } catch (Exception $e) {
            Log::warning("Ошибка доктрины: " . $e->getMessage());
            $this->createTableUsingRawSql($externalDb, $connectionNameUpload, $table, $downloadDbData['schema'] ?? 'public');
            return;
        }

        $this->createOrUpdateTableSchema($schemaTable, $table, $connectionNameUpload);

        Log::debug("Окончена синхронизация схемы из $schema." . $downloadDbData['database'] . " в $uploadDbSchema." . $uploadDbData['database']);
    }

    private function createOrUpdateTableSchema(Table $schemaTable, string $table, string $connectionNameUpload): void
    {
        try {
            if (!Schema::connection($connectionNameUpload)->hasTable($table)) {
                $this->createTableSchema($schemaTable, $table, $connectionNameUpload);
            } else {
                $this->updateTableSchema($schemaTable, $table, $connectionNameUpload);
            }
        } catch (\Exception $e) {
            Log::error("Ошибка при создании/изменении таблицы: " . $e->getMessage() . " в строке " . $e->getLine());
        }
    }

    private function createTableSchema(Table $schemaTable, string $table, string $connectionNameUpload): void
    {
        Schema::connection($connectionNameUpload)->create($table, function (Blueprint $tableBlueprint) use ($schemaTable): void {
            foreach ($schemaTable->getColumns() as $column) {
                $type = $column->getType()->getName();
                $name = $column->getName();
                $tableBlueprint->addColumn($this->convertTypeToPostgres($type), $name, $this->formParams($column->toArray()));
            }

            foreach ($schemaTable->getIndexes() as $index) {
                if ($index->isPrimary()) {
                    $tableBlueprint->primary($index->getColumns());
                } elseif ($index->isUnique()) {
                    $tableBlueprint->unique($index->getColumns(), $index->getName());
                } else {
                    $tableBlueprint->index($index->getColumns(), $index->getName());
                }
            }
        });
    }

    private function updateTableSchema(Table $schemaTable, string $table, string $connectionNameUpload): void
    {
        Schema::connection($connectionNameUpload)->table($table, function (Blueprint $tableBlueprint) use ($schemaTable, $table, $connectionNameUpload): void {
            $existingColumns = Schema::connection($connectionNameUpload)->getColumnListing($table);

            foreach ($schemaTable->getColumns() as $column) {
                $type = $column->getType()->getName();
                $name = $column->getName();

                if (!in_array($name, $existingColumns)) {
                    $tableBlueprint->addColumn($this->convertTypeToPostgres($type), $name, $this->formParams($column->toArray()));
                } else {
                    $newType = $this->getPostgresType($this->formParams($column->toArray())['type']);

                    $query = "ALTER TABLE \"$table\" ALTER COLUMN \"$name\" TYPE $newType";
                    DB::connection($connectionNameUpload)->statement($query);
                }
            }

            foreach ($existingColumns as $existingColumn) {
                if (!$schemaTable->hasColumn($existingColumn)) {
                    $tableBlueprint->dropColumn($existingColumn);
                }
            }
        });
    }

    private function syncFullSchema(SyncDb $database): void
    {
        $this->syncSchema($database);
        $this->processTables($database, 'full');
    }

    private function processTables(SyncDb $database, string $type): void
    {
        $databaseData = $this->getDataFromModel($database->data);
        $connectionName = $this->configureConnection($databaseData, $database->type);

        $externalDb = DB::connection($connectionName);
        $tables = $this->filterTables($externalDb->getDoctrineSchemaManager()->listTableNames());

        $schema = $databaseData['schema'] ?? 'public';

        $uploadDb = SyncSchedule::where('db', $database->id)->first();
        $uploadDbData = $this->getDataFromModel($uploadDb->data);
        $uploadDbSchema = $uploadDbData['schema'] ?? 'public';
        $connectionNameUpload = $this->configureConnection($uploadDbData, $uploadDbData['type'] ?? 'pgsql');

        foreach ($tables as $table) {
            $context = new SyncContext(
                $externalDb,
                $databaseData,
                $table,
                $database->id,
                $schema,
                $uploadDbSchema,
                $type,
                $uploadDbData,
                $connectionNameUpload
            );
            $this->syncTableData($context);
        }
    }


    private function syncTableData(SyncContext $context): void
    {
        Log::debug("Начата синхронизация данных {$context->table} из {$context->schema}." . $context->databaseData['database'] . " в {$context->uploadDbSchema}." . $context->uploadDbData['database']);

        try {
            $schemaManager = $context->externalDb->getDoctrineSchemaManager();
            $updatedAtField = $context->databaseData['updated_at_fields'][$context->table] ?? 'updated_at';
            $query = $context->externalDb->table($context->table);

            if ($context->type === 'update') {
                $lastSync = SyncUpdate::where('db', $context->databaseId)
                    ->where('table', $context->table)
                    ->where('schema', $context->schema)
                    ->value('updated');

                if ($lastSync) {
                    $query->orderBy($updatedAtField)->where($updatedAtField, '>', $lastSync);
                }
            }

            $firstColumn = $context->externalDb->getSchemaBuilder()->getColumnListing($context->table)[0];
            $chunkSize = $this->getChunkSize();
            $query->orderBy($firstColumn)->chunk($chunkSize, function ($records) use ($context, $schemaManager): void {
                foreach ($records as $record) {
                    $uniqueColumns = $this->getUniqueColumns($schemaManager, $context->table);
                    DB::connection($context->connectionNameUpload)->table($context->table)->upsert((array)$record, $uniqueColumns);
                    $this->updateCachekey($this->getDataFromModel($context->databaseData));
                    $this->updateSyncTimestamp($context->databaseId, $context->table, Carbon::now(), $context->schema);
                }
            });

            Log::debug("Окончена синхронизация данных {$context->table} из {$context->schema}." . $context->databaseData['database'] . " в {$context->uploadDbSchema}." . $context->uploadDbData['database']);
        } catch (\Exception $e) {
            Log::error("Ошибка синхронизации таблицы {$context->table}: " . $e->getMessage(), [
                'database_id' => $context->databaseId,
                'table' => $context->table,
            ]);
        }
    }


    private function getUniqueColumns(SchemaManager $schemaManager, string $table, string $defaultColumn = 'id'): array
    {
        try {
            $tableDetails = $schemaManager->listTableDetails($table);
            $primaryKey = $tableDetails->getPrimaryKey();
            if ($primaryKey) {
                return $primaryKey->getColumns();
            }

            $uniqueIndexes = $tableDetails->getIndexes();
            foreach ($uniqueIndexes as $index) {
                if ($index->isUnique() && !$index->isPrimary()) {
                    return $index->getColumns();
                }
            }

            return [$defaultColumn]; // Вернуть по умолчанию, если ничего не найдено
        } catch (Exception $e) {
            Log::warning("Не удалось получить уникальные столбцы для таблицы $table: " . $e->getMessage());
            return [$defaultColumn];
        }
    }

    private function updateSyncTimestamp(string $dbId, string $table, Carbon $updated, string $schema = 'public'): void
    {
        SyncUpdate::updateOrInsert(
            ['db' => $dbId, 'table' => $table, 'schema' => $schema],
            ['updated' => $updated, 'id' => Str::uuid()->toString(), 'created_at' => now()]
        );
    }

    private function createTableUsingRawSql(Connection $externalDb, string $connectionNameUpload, string $tableName, string $schemaName): bool
    {
        try {
            $tableStructure = $this->getTableStructure($externalDb, $tableName, $schemaName);
            $tableExists = $this->checkIfTableExists($connectionNameUpload, $tableName, $schemaName);

            if (!$tableExists) {
                $createQuery = $this->buildCreateTableQuery($tableStructure, $tableName);
                DB::connection($connectionNameUpload)->statement($createQuery);
                Log::debug("Сырой запрос для таблицы {$tableName}: $createQuery");
                return true;
            } else {
                $this->updateTableStructure($externalDb, $connectionNameUpload, $tableName, $schemaName, $tableStructure);
                return true;
            }
        } catch (\Exception $e) {
            Log::warning("Ошибка при сыром запросе для таблицы {$tableName}: " . $e->getMessage());
            return false;
        }
    }

    private function getTableStructure(Connection $externalDb, string $tableName, string $schemaName): array
    {
        $sql = "SELECT *
                FROM information_schema.columns
                WHERE table_name = :table_name AND table_schema = :schema_name";

        return $externalDb->select($sql, ['table_name' => $tableName, 'schema_name' => $schemaName]);
    }

    private function checkIfTableExists(string $connectionNameUpload, string $tableName, string $schemaName): bool
    {
        $tableExistsQuery = "
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = :table_name AND table_schema = :schema_name
            ) as exists;";

        $result = DB::connection($connectionNameUpload)->select($tableExistsQuery, ['table_name' => $tableName, 'schema_name' => $schemaName]);
        return $result[0]->exists;
    }

    private function buildCreateTableQuery(array $tableStructure, string $tableName): string
    {
        $queryCreateTable = "CREATE TABLE IF NOT EXISTS \"$tableName\" (";
        $primaryKey = null;

        foreach ($tableStructure as $column) {
            $columnName = $column->column_name;
            $dataType = $column->data_type;

            if (strtolower($dataType) == 'user-defined') { // костыль пост гис
                $dataType = 'text';
            }

            $queryCreateTable .= "\"$columnName\" $dataType";

            if ($column->is_nullable === 'YES') {
                $queryCreateTable .= ' NULL';
            } else {
                $queryCreateTable .= ' NOT NULL';
            }

            if (!is_null($column->column_default)) {
                $queryCreateTable .= " DEFAULT $column->column_default";
            }

            if ($columnName === 'id') {
                $primaryKey = "\"$columnName\"";
            }

            $queryCreateTable .= ",";
        }

        $queryCreateTable = rtrim($queryCreateTable, ',');

        if ($primaryKey !== null) {
            $queryCreateTable .= ", PRIMARY KEY ($primaryKey)";
        }

        $queryCreateTable .= ");";

        return $queryCreateTable;
    }

    private function updateTableStructure(Connection $externalDb, string $connectionNameUpload, string $tableName, string $schemaName, array $newColumns): void
    {
        $currentColumns = $this->getCurrentTableColumns($connectionNameUpload, $tableName);

        foreach ($newColumns as $newColumn) {
            $this->addOrUpdateColumn($connectionNameUpload, $tableName, $newColumn, $currentColumns);
        }

        $this->dropRemovedColumns($connectionNameUpload, $tableName, $currentColumns, $newColumns);
    }

    private function getCurrentTableColumns(string $connectionNameUpload, string $tableName): array
    {
        $currentColumnsQuery = "
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = :table_name";

        return DB::connection($connectionNameUpload)->select($currentColumnsQuery, ['table_name' => $tableName]);
    }

    private function addOrUpdateColumn(string $connectionNameUpload, string $tableName, $newColumn, array $currentColumns): void
    {
        $columnName = $newColumn->column_name;
        $columnType = $newColumn->data_type;
        $columnExists = false;

        foreach ($currentColumns as $currentColumn) {
            if ($currentColumn->column_name === $columnName) {
                $columnExists = true;
                if ($currentColumn->data_type !== $columnType) {
                    if (strtolower($columnType) == 'user-defined') { // костыль пост гис
                        $columnType = 'text';
                    }

                    $alterQuery = "
                        ALTER TABLE \"$tableName\"
                        ALTER COLUMN \"$columnName\" TYPE $columnType;";
                    DB::connection($connectionNameUpload)->statement($alterQuery);
                }
            }
        }

        if (!$columnExists) {
            if (strtolower($columnType) == 'user-defined') { // костыль пост гис
                $columnType = 'text';
            }

            $addColumnQuery = "
                ALTER TABLE \"$tableName\"
                ADD COLUMN \"$columnName\" $columnType;";
            DB::connection($connectionNameUpload)->statement($addColumnQuery);
        }
    }

    private function dropRemovedColumns(string $connectionNameUpload, string $tableName, array $currentColumns, array $newColumns): void
    {
        foreach ($currentColumns as $currentColumn) {
            $columnName = $currentColumn->column_name;
            $columnExistsInNew = false;

            foreach ($newColumns as $newColumn) {
                if ($newColumn->column_name === $columnName) {
                    $columnExistsInNew = true;
                    break;
                }
            }

            if (!$columnExistsInNew) {
                $dropColumnQuery = "
                    ALTER TABLE \"$tableName\"
                    DROP COLUMN \"$columnName\";";
                DB::connection($connectionNameUpload)->statement($dropColumnQuery);
            }
        }
    }

    private function getDataFromModel(string|array $data): array
    {
        return [];
    }

    private function generateCacheKey(array $connectionDetails): string
    {
        return '';
    }

    private function installExtensionsUploadDb(array $data): void
    {
        return;
    }

    private function getSyncDuration(): int
    {
        return 0;
    }

    private function configureConnection(array $data, string $type): string
    {
        return '';
    }

    private function filterTables(array $tables): array
    {
        return [];
    }

    private function convertTypeToPostgres(string $type): string
    {
        return '';
    }

    private function formParams(array $column): array
    {
        return [];
    }

    private function getPostgresType(string $type): string
    {
        return '';
    }

    private function getChunkSize(): int
    {
        return 0;
    }

    private function updateCachekey(array $data): void
    {
        return;
    }

    private function logSuccess(SyncSchedule $schedule, Carbon $startTime): void
    {
        return;
    }
}
