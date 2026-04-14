param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('iceberg', 'paimon')]
    [string]$Engine,

    [string]$PDate = '2024-06-14',

    [string]$FlinkHome = 'D:\softDir\flink\flink-1.18.1',

    [string]$MetastoreLibDir = 'D:\softDir\flink\hive-metastore-libs-curated',

    [string]$LocalHiveConfFile = 'D:\softDir\hive-local\conf\hive-site.xml',

    [string]$LocalWarehouseRoot = 'D:\softDir\hive-local\warehouse',

    [int]$TimeoutSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($PDate -ne '2024-06-14') {
    throw "This local alignment test is fixed to 2024-06-14. Current PDate: $PDate"
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$targetRoot = Join-Path $repoRoot 'target'
$engineTargetRoot = Join-Path $targetRoot ("local-" + $Engine + "-alignment")
$startMetastoreScript = Join-Path $PSScriptRoot 'start-local-hive-metastore.ps1'
$startFlinkClusterScript = Join-Path $PSScriptRoot 'start-local-flink-cluster.ps1'
$runSqlScript = Join-Path $PSScriptRoot 'run-hudi-flink-sql.ps1'
$resetSqlFile = Join-Path $PSScriptRoot 'hudi-local-reset-dws-alignment-tables.sql'
$seedSqlFile = Join-Path $PSScriptRoot 'hudi-local-seed-dws-alignment-source-tables.sql'
$verifySqlFile = Join-Path $PSScriptRoot 'hudi-local-verify-dws-alignment.sql'

$engineConfigs = @{
    iceberg = @{
        CatalogName = 'iceberg_catalog'
        CatalogBlockTemplate = @'
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = '{WAREHOUSE_URI}'
);
'@
        TableOptionsTemplate = @'
) PARTITIONED BY (`k1`) WITH (
    'format-version' = '2'
);
'@
        DbMap = @{
            'hudi_dws' = 'iceberg_dws'
            'hudi_dim' = 'iceberg_dim'
            'hudi_dwd' = 'iceberg_dwd'
            'hudi_ods' = 'iceberg_ods'
        }
        HadoopLibDir = Join-Path $targetRoot 'iceberg-test-libs'
        WarehouseDirs = @(
            'iceberg_dws.db',
            'iceberg_dim.db',
            'iceberg_dwd.db',
            'iceberg_test_local.db'
        )
        TargetSqlFiles = @(
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_trade_user_cart_add_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_trade_user_payment_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_trade_user_sku_order_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_trade_user_sku_order_refund_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_traffic_page_visitor_page_view_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_trade_user_order_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_trade_user_order_td_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws\dws_user_user_login_td_full.sql'
        )
    }
    paimon = @{
        CatalogName = 'paimon_hive'
        CatalogBlockTemplate = @'
CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = '{METASTORE_URI}',
    'warehouse' = '{WAREHOUSE_URI}'
);
'@
        TableOptionsTemplate = @'
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'sink.parallelism' = '1',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);
'@
        DbMap = @{
            'hudi_dws' = 'dws'
            'hudi_dim' = 'dim'
            'hudi_dwd' = 'dwd'
            'hudi_ods' = 'ods'
        }
        HadoopLibDir = Join-Path $targetRoot 'paimon-test-libs-08'
        WarehouseDirs = @(
            'dws.db',
            'dim.db',
            'dwd.db'
        )
        TargetSqlFiles = @(
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_trade_user_cart_add_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_trade_user_payment_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_trade_user_sku_order_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_trade_user_sku_order_refund_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_traffic_page_visitor_page_view_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_trade_user_order_nd_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_trade_user_order_td_full.sql',
            'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws\dws_user_user_login_td_full.sql'
        )
    }
}

$expectedLines = @(
    'cart_add_nd|900001|2024-06-14|3|8|7|18',
    'cart_add_nd|900002|2024-06-14|3|4|8|12',
    'cart_add_nd|900003|2024-06-14|0|0|2|6',
    'payment_nd|900001|2024-06-14|3|5|170.00|7|10|370.00',
    'payment_nd|900002|2024-06-14|1|1|20.00|4|5|120.00',
    'payment_nd|900003|2024-06-14|0|0|0.00|2|2|60.00',
    'sku_order_nd|900001|101|2024-06-14|sku-101-new|11|c1-new|21|c2-new|31|c3-new|1|tm-1|3|4|150.00|10.00|5.00|135.00|7|9|350.00|30.00|15.00|305.00',
    'sku_order_nd|900001|102|2024-06-14|sku-102|12|c1-12|22|c2-22|32|c3-32|2|tm-2|1|1|40.00|4.00|1.00|35.00|2|2|60.00|4.00|3.00|53.00',
    'sku_order_nd|900002|101|2024-06-14|sku-101-new|11|c1-new|21|c2-new|31|c3-new|1|tm-1|1|1|30.00|3.00|1.00|26.00|1|1|30.00|3.00|1.00|26.00',
    'sku_refund_nd|900001|101|2024-06-14|sku-101-new|11|c1-new|21|c2-new|31|c3-new|1|tm-1|1|1|30.00|2|4|120.00',
    'sku_refund_nd|900001|102|2024-06-14|sku-102|12|c1-12|22|c2-22|32|c3-32|2|tm-2|2|2|50.00|2|2|50.00',
    'sku_refund_nd|900002|101|2024-06-14|sku-101-new|11|c1-new|21|c2-new|31|c3-new|1|tm-1|1|1|20.00|1|1|20.00',
    'traffic_nd|mid-1|detail|2024-06-14|brand-a|model-x|android|30|1|30|1',
    'traffic_nd|mid-1|home|2024-06-14|brand-a|model-x|android|150|3|350|7',
    'traffic_nd|mid-2|home|2024-06-14|brand-c|model-z|android|0|0|60|1',
    'order_nd|900001|2024-06-14|3|4|150.00|10.00|5.00|135.00|7|9|350.00|30.00|15.00|305.00',
    'order_nd|900002|2024-06-14|3|3|90.00|9.00|0.00|81.00|10|11|250.00|13.00|21.00|216.00',
    'order_nd|900003|2024-06-14|0|0|0.00|0.00|0.00|0.00|1|2|30.00|3.00|1.00|26.00',
    'order_td|900001|2024-06-14|2024-05-15|2024-06-14|14|16|1050.00|100.00|50.00|900.00',
    'order_td|900002|2024-06-14|2024-05-15|2024-06-08|11|12|261.00|14.00|22.00|225.00',
    'order_td|900003|2024-06-14|2024-05-25|2024-05-25|1|2|30.00|3.00|1.00|26.00',
    'login_td|1001|2024-06-14|2024-06-14|3',
    'login_td|1002|2024-06-14|2024-06-08|2',
    'login_td|1003|2024-06-14|2024-06-13|1',
    'login_td|1004|2024-06-14|2024-06-09|1'
)

function Write-Utf8NoBomFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    $directory = Split-Path -Path $Path -Parent
    if ($directory -and -not (Test-Path -LiteralPath $directory)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }

    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $encoding)
}

function Invoke-RepoScript {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,

        [hashtable]$Parameters = @{}
    )

    return (& $ScriptPath @Parameters 2>&1 | Out-String)
}

function Get-LocalMetastoreUri {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HiveSitePath
    )

    if (-not (Test-Path -LiteralPath $HiveSitePath)) {
        throw "Hive site file not found: $HiveSitePath"
    }

    [xml]$xml = Get-Content -LiteralPath $HiveSitePath
    $uriNode = $xml.configuration.property | Where-Object { $_.name -eq 'hive.metastore.uris' } | Select-Object -First 1
    if (-not $uriNode -or -not $uriNode.value) {
        throw "Failed to read hive.metastore.uris from $HiveSitePath"
    }

    return [string]$uriNode.value
}

function Remove-TestWarehouseDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$WarehouseRoot,

        [Parameter(Mandatory = $true)]
        [string]$RelativePath
    )

    $basePath = [System.IO.Path]::GetFullPath($WarehouseRoot)
    $fullPath = [System.IO.Path]::GetFullPath((Join-Path $WarehouseRoot $RelativePath))
    if (-not $fullPath.StartsWith($basePath, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove path outside of local test warehouse: $fullPath"
    }

    if (Test-Path -LiteralPath $fullPath) {
        Remove-Item -LiteralPath $fullPath -Recurse -Force
    }
}

function Get-EngineLocalSql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent,

        [Parameter(Mandatory = $true)]
        [hashtable]$EngineConfig,

        [Parameter(Mandatory = $true)]
        [string]$MetastoreUri,

        [Parameter(Mandatory = $true)]
        [string]$WarehouseUri
    )

    $catalogBlock = $EngineConfig.CatalogBlockTemplate.Replace('{METASTORE_URI}', $MetastoreUri).Replace('{WAREHOUSE_URI}', $WarehouseUri)
    $tableOptions = $EngineConfig.TableOptionsTemplate.Replace('{METASTORE_URI}', $MetastoreUri).Replace('{WAREHOUSE_URI}', $WarehouseUri)

    $content = $SqlContent
    $content = [regex]::Replace($content, '(?is)create\s+catalog\s+\w+\s+with\s*\(\s*.*?\s*\);\s*', $catalogBlock + "`r`n")
    $content = [regex]::Replace($content, '(?im)^\s*use\s+catalog\s+\w+\s*;\s*$', 'USE CATALOG ' + $EngineConfig.CatalogName + ';')

    foreach ($entry in $EngineConfig.DbMap.GetEnumerator()) {
        $content = [regex]::Replace($content, "\b$([regex]::Escape($entry.Key))\b", $entry.Value)
    }

    $content = [regex]::Replace($content, "\s*/\*\+\s*OPTIONS\('read\.streaming\.enabled'\s*=\s*'false'\)\s*\*/", '')
    $content = [regex]::Replace($content, "'uri'\s*=\s*'thrift://[^']+'", "'uri' = '" + $MetastoreUri + "'")
    $content = [regex]::Replace($content, "'warehouse'\s*=\s*'[^']*user/hive/warehouse/?'", "'warehouse' = '" + $WarehouseUri + "'")
    $content = [regex]::Replace($content, '(?is)\)\s*PARTITIONED BY\s*\(\s*`k1`\s*\)\s*WITH\s*\(\s*.*?\s*\);', $tableOptions)

    return $content
}

function Ensure-TestLibDir {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EngineName,

        [Parameter(Mandatory = $true)]
        [string]$DestinationDir
    )

    if (Test-Path -LiteralPath $DestinationDir) {
        return
    }

    New-Item -ItemType Directory -Path $DestinationDir -Force | Out-Null
    Copy-Item 'D:\softDir\flink\hive-metastore-libs-curated\*' -Destination $DestinationDir -Force

    switch ($EngineName) {
        'iceberg' {
            Copy-Item 'D:\softDir\xunleidownload\iceberg-flink-runtime-1.18-1.5.2.jar' -Destination $DestinationDir -Force
            Copy-Item 'D:\softDir\xunleidownload\iceberg-hive-runtime-1.5.2.jar' -Destination $DestinationDir -Force
        }
        'paimon' {
            Copy-Item 'D:\softDir\xunleidownload\paimon-flink-1.18-0.8-20240301.002155-30.jar' -Destination $DestinationDir -Force
        }
    }
}

$engineConfig = $engineConfigs[$Engine]
Ensure-TestLibDir -EngineName $Engine -DestinationDir $engineConfig.HadoopLibDir

if (-not (Test-Path -LiteralPath $engineTargetRoot)) {
    New-Item -ItemType Directory -Path $engineTargetRoot -Force | Out-Null
}

Write-Output 'Starting local Hive Metastore if needed...'
Invoke-RepoScript -ScriptPath $startMetastoreScript -Parameters @{
    LibDir = $MetastoreLibDir
} | Out-Null

$metastoreUri = Get-LocalMetastoreUri -HiveSitePath $LocalHiveConfFile
$warehouseUri = 'file:///D:/softDir/hive-local/warehouse'

Write-Output 'Starting local Flink cluster if needed...'
Invoke-RepoScript -ScriptPath $startFlinkClusterScript -Parameters @{
    FlinkHome = $FlinkHome
    HadoopLibDir = $engineConfig.HadoopLibDir
    ForceRestart = $true
} | Out-Null

$renderedResetSql = Join-Path $engineTargetRoot 'reset.sql'
$renderedSeedSql = Join-Path $engineTargetRoot 'seed.sql'
$renderedVerifySql = Join-Path $engineTargetRoot 'verify.sql'

Write-Utf8NoBomFile -Path $renderedResetSql -Content (Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $resetSqlFile -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri)
Write-Utf8NoBomFile -Path $renderedSeedSql -Content (Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $seedSqlFile -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri)
Write-Utf8NoBomFile -Path $renderedVerifySql -Content (Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $verifySqlFile -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri)

$renderedTargetSqlFiles = @()
foreach ($relativePath in $engineConfig.TargetSqlFiles) {
    $sourcePath = Join-Path $repoRoot $relativePath
    $targetPath = Join-Path $engineTargetRoot ([System.IO.Path]::GetFileName($sourcePath))
    Write-Utf8NoBomFile -Path $targetPath -Content (Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $sourcePath -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri)
    $renderedTargetSqlFiles += $targetPath
}

Write-Output 'Dropping local alignment test tables from metastore...'
Invoke-RepoScript -ScriptPath $runSqlScript -Parameters @{
    SqlFile = $renderedResetSql
    FlinkHome = $FlinkHome
    HadoopLibDir = $engineConfig.HadoopLibDir
    TimeoutSeconds = $TimeoutSeconds
} | Out-Null

Write-Output 'Removing local alignment test directories...'
foreach ($relativeDir in $engineConfig.WarehouseDirs) {
    Remove-TestWarehouseDirectory -WarehouseRoot $LocalWarehouseRoot -RelativePath $relativeDir
}

Write-Output 'Seeding source tables for alignment test...'
Invoke-RepoScript -ScriptPath $runSqlScript -Parameters @{
    SqlFile = $renderedSeedSql
    FlinkHome = $FlinkHome
    HadoopLibDir = $engineConfig.HadoopLibDir
    TimeoutSeconds = $TimeoutSeconds
    WaitForCompletion = $true
    JobWaitTimeoutSeconds = $TimeoutSeconds
    LogFile = (Join-Path $engineTargetRoot 'seed.log')
} | Out-Null

foreach ($targetSqlFile in $renderedTargetSqlFiles) {
    $targetName = Split-Path -Path $targetSqlFile -Leaf
    Write-Output ("Executing " + $targetName + ' ...')
    Invoke-RepoScript -ScriptPath $runSqlScript -Parameters @{
        SqlFile = $targetSqlFile
        PDate = $PDate
        FlinkHome = $FlinkHome
        HadoopLibDir = $engineConfig.HadoopLibDir
        TimeoutSeconds = $TimeoutSeconds
        WaitForCompletion = $true
        JobWaitTimeoutSeconds = $TimeoutSeconds
        LogFile = (Join-Path $engineTargetRoot ("run-" + $targetName + ".log"))
    } | Out-Null
}

Write-Output 'Querying verification rows from target tables...'
$verifyOutput = Invoke-RepoScript -ScriptPath $runSqlScript -Parameters @{
    SqlFile = $renderedVerifySql
    FlinkHome = $FlinkHome
    HadoopLibDir = $engineConfig.HadoopLibDir
    TimeoutSeconds = $TimeoutSeconds
    LogFile = (Join-Path $engineTargetRoot 'verify.log')
}

Write-Output $verifyOutput.Trim()

$missingLines = @()
foreach ($expectedLine in $expectedLines) {
    if (-not $verifyOutput.Contains($expectedLine)) {
        $missingLines += $expectedLine
    }
}

if ($missingLines.Count -gt 0) {
    $missingText = $missingLines -join [Environment]::NewLine
    throw "Verification failed. Missing expected rows:`n$missingText"
}

Write-Output ($Engine + ' alignment verification passed.')
