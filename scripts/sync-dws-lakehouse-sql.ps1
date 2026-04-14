param(
    [string]$ProjectRoot = 'D:\data-warehouse-learning\data-warehouse-learning-master'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$hudiDir = Join-Path $ProjectRoot 'src\main\java\org\bigdatatechcir\warehouse\flink\hudi\dws'

$engineConfigs = @(
    @{
        Name = 'iceberg'
        TargetDir = Join-Path $ProjectRoot 'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws'
        CatalogBlock = @'
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

'@
        UseCatalog = 'USE CATALOG iceberg_catalog;'
        Database = 'CREATE DATABASE IF NOT EXISTS iceberg_dws;'
        TableOptions = @'
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

'@
        IdentifierMap = @{
            'hudi_dws' = 'iceberg_dws'
            'hudi_dwd' = 'iceberg_dwd'
            'hudi_dim' = 'iceberg_dim'
            'hudi_ods' = 'iceberg_ods'
        }
        InsertPattern = '(?m)^INSERT INTO iceberg_dws\.([^\s(]+)\('
        InsertReplacement = "INSERT INTO iceberg_dws.`$1 /*+ OPTIONS('upsert-enabled' = 'true') */("
    },
    @{
        Name = 'paimon'
        TargetDir = Join-Path $ProjectRoot 'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws'
        CatalogBlock = @'
CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);

'@
        UseCatalog = 'USE CATALOG paimon_hive;'
        Database = 'CREATE DATABASE IF NOT EXISTS dws;'
        TableOptions = @'
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);
'@
        IdentifierMap = @{
            'hudi_dws' = 'dws'
            'hudi_dwd' = 'dwd'
            'hudi_dim' = 'dim'
            'hudi_ods' = 'ods'
        }
        InsertPattern = $null
        InsertReplacement = $null
    }
)

if (-not (Test-Path -LiteralPath $hudiDir)) {
    throw "Hudi DWS directory not found: $hudiDir"
}

$catalogPattern = '(?is)create\s+catalog\s+hudi_catalog\s+with\s*\(\s*.*?\s*\);\s*'
$useCatalogPattern = '(?is)\buse\s+catalog\s+hudi_catalog\s*;'
$databasePattern = '(?is)\bcreate\s+database\s+if\s+not\s+exists\s+hudi_dws\s*;'
$tableOptionsPattern = '(?is)\)\s*PARTITIONED BY\s*\(\s*`k1`\s*\)\s*WITH\s*\(\s*.*?\s*\);'
$hudiReadHintPattern = "\s*/\*\+\s*OPTIONS\('read\.streaming\.enabled'\s*=\s*'false'\)\s*\*/"
$tableOptionsRegex = [regex]::new($tableOptionsPattern)

$sourceFiles = Get-ChildItem -Path $hudiDir -Filter '*.sql' | Sort-Object Name

foreach ($engine in $engineConfigs) {
    foreach ($sourceFile in $sourceFiles) {
        $content = Get-Content -LiteralPath $sourceFile.FullName -Raw

        $content = [regex]::Replace($content, $catalogPattern, $engine.CatalogBlock)
        $content = [regex]::Replace($content, $useCatalogPattern, $engine.UseCatalog)
        $content = [regex]::Replace($content, $databasePattern, $engine.Database)

        foreach ($entry in $engine.IdentifierMap.GetEnumerator()) {
            $content = [regex]::Replace($content, "\b$([regex]::Escape($entry.Key))\b", $entry.Value)
        }

        $content = [regex]::Replace($content, $hudiReadHintPattern, '')
        $content = $tableOptionsRegex.Replace($content, $engine.TableOptions, 1)

        if ($engine.InsertPattern) {
            $insertRegex = [regex]::new($engine.InsertPattern)
            $content = $insertRegex.Replace($content, $engine.InsertReplacement, 1)
        }

        $targetPath = Join-Path $engine.TargetDir $sourceFile.Name
        Set-Content -LiteralPath $targetPath -Value $content -Encoding UTF8
    }
}

Write-Host "Synchronized $($sourceFiles.Count) DWS SQL files from hudi to iceberg and paimon."
