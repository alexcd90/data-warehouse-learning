param(
    [ValidateSet('all', 'hudi', 'paimon', 'iceberg')]
    [string]$Engine = 'all',

    [string]$PDate = '2024-06-14',

    [string]$FlinkHome = 'D:\softDir\flink\flink-1.18.1',

    [string]$HudiHadoopLibDir = 'D:\softDir\flink\hudi-hms-test-libs-curated',

    [string]$MetastoreLibDir = 'D:\softDir\flink\hive-metastore-libs-curated',

    [string]$LocalHiveConfFile = 'D:\softDir\hive-local\conf\hive-site.xml',

    [string]$LocalWarehouseRoot = 'D:\softDir\hive-local\warehouse',

    [int]$TimeoutSeconds = 1800,

    [int]$RestartFlinkEveryTargetSqlCount = 6
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($PDate -ne '2024-06-14') {
    throw "This local full-suite test is fixed to 2024-06-14. Current PDate: $PDate"
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$targetRoot = Join-Path $repoRoot 'target'
$startMetastoreScript = Join-Path $PSScriptRoot 'start-local-hive-metastore.ps1'
$startFlinkClusterScript = Join-Path $PSScriptRoot 'start-local-flink-cluster.ps1'
$runSqlScript = Join-Path $PSScriptRoot 'run-hudi-flink-sql.ps1'
$resetSqlFile = Join-Path $PSScriptRoot 'hudi-local-reset-dws-full-suite-tables.sql'
$seedSqlFile = Join-Path $PSScriptRoot 'hudi-local-seed-dws-full-suite-source-tables.sql'
$verifySqlFile = Join-Path $PSScriptRoot 'hudi-local-verify-dws-full-suite.sql'

$expectedTableNames = @(
    'dws_trade_activity_order_nd_full',
    'dws_trade_coupon_order_nd_full',
    'dws_trade_province_order_1d_full',
    'dws_trade_province_order_nd_full',
    'dws_trade_user_cart_add_1d_full',
    'dws_trade_user_cart_add_nd_full',
    'dws_trade_user_order_1d_full',
    'dws_trade_user_order_nd_full',
    'dws_trade_user_order_refund_1d_full',
    'dws_trade_user_order_refund_nd_full',
    'dws_trade_user_order_td_full',
    'dws_trade_user_payment_1d_full',
    'dws_trade_user_payment_nd_full',
    'dws_trade_user_sku_order_1d_full',
    'dws_trade_user_sku_order_nd_full',
    'dws_trade_user_sku_order_refund_1d_full',
    'dws_trade_user_sku_order_refund_nd_full',
    'dws_traffic_page_visitor_page_view_1d_full',
    'dws_traffic_page_visitor_page_view_nd_full',
    'dws_traffic_session_page_view_1d_full',
    'dws_user_user_login_td_full'
)

$hudiWarehouseDirs = @(
    'hudi_dws\dws_trade_activity_order_nd_full',
    'hudi_dws\dws_trade_coupon_order_nd_full',
    'hudi_dws\dws_trade_province_order_1d_full',
    'hudi_dws\dws_trade_province_order_nd_full',
    'hudi_dws\dws_trade_user_cart_add_1d_full',
    'hudi_dws\dws_trade_user_cart_add_nd_full',
    'hudi_dws\dws_trade_user_order_1d_full',
    'hudi_dws\dws_trade_user_order_nd_full',
    'hudi_dws\dws_trade_user_order_refund_1d_full',
    'hudi_dws\dws_trade_user_order_refund_nd_full',
    'hudi_dws\dws_trade_user_order_td_full',
    'hudi_dws\dws_trade_user_payment_1d_full',
    'hudi_dws\dws_trade_user_payment_nd_full',
    'hudi_dws\dws_trade_user_sku_order_1d_full',
    'hudi_dws\dws_trade_user_sku_order_nd_full',
    'hudi_dws\dws_trade_user_sku_order_refund_1d_full',
    'hudi_dws\dws_trade_user_sku_order_refund_nd_full',
    'hudi_dws\dws_traffic_page_visitor_page_view_1d_full',
    'hudi_dws\dws_traffic_page_visitor_page_view_nd_full',
    'hudi_dws\dws_traffic_session_page_view_1d_full',
    'hudi_dws\dws_user_user_login_td_full',
    'hudi_dim\dim_activity_full',
    'hudi_dim\dim_coupon_full',
    'hudi_dim\dim_province_full',
    'hudi_dim\dim_sku_full',
    'hudi_dim\dim_user_zip_full',
    'hudi_dwd\dwd_tool_coupon_order_full',
    'hudi_dwd\dwd_trade_cart_add_full',
    'hudi_dwd\dwd_trade_order_detail_full',
    'hudi_dwd\dwd_trade_order_refund_full',
    'hudi_dwd\dwd_trade_pay_detail_suc_full',
    'hudi_dwd\dwd_traffic_page_view_full',
    'hudi_dwd\dwd_user_login_full'
)

$engineConfigs = @{
    hudi = @{
        PassThrough = $true
        HadoopLibDir = $HudiHadoopLibDir
        DisableHiveSync = $true
        WarehouseDirs = $hudiWarehouseDirs
        TargetSqlDir = Join-Path $repoRoot 'src\main\java\org\bigdatatechcir\warehouse\flink\hudi\dws'
    }
    paimon = @{
        PassThrough = $false
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
        UnpartitionedTableOptionsTemplate = @'
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'sink.parallelism' = '1',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);
'@
        DbMap = @{
            'hudi_dws' = 'dws'
            'hudi_dim' = 'dim'
            'hudi_dwd' = 'dwd'
            'hudi_ods' = 'ods'
        }
        HadoopLibDir = Join-Path $targetRoot 'paimon-test-libs-08'
        DisableHiveSync = $false
        WarehouseDirs = @('dws.db', 'dim.db', 'dwd.db')
        TargetSqlDir = Join-Path $repoRoot 'src\main\java\org\bigdatatechcir\warehouse\flink\paimon\dws'
    }
    iceberg = @{
        PassThrough = $false
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
        UnpartitionedTableOptionsTemplate = @'
) WITH (
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
        DisableHiveSync = $false
        WarehouseDirs = @('iceberg_dws.db', 'iceberg_dim.db', 'iceberg_dwd.db')
        TargetSqlDir = Join-Path $repoRoot 'src\main\java\org\bigdatatechcir\warehouse\flink\iceberg\dws'
    }
}

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

function Restart-LocalFlinkCluster {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EngineName,

        [Parameter(Mandatory = $true)]
        [hashtable]$EngineConfig,

        [Parameter(Mandatory = $true)]
        [string]$FlinkHomePath
    )

    Write-Output "Restarting local Flink cluster for $EngineName ..."
    Invoke-RepoScript -ScriptPath $startFlinkClusterScript -Parameters @{
        FlinkHome = $FlinkHomePath
        HadoopLibDir = $EngineConfig.HadoopLibDir
        ForceRestart = $true
    } | Out-Null
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

    if ($EngineConfig.PassThrough) {
        return $SqlContent
    }

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
    $content = [regex]::Replace($content, "'warehouse'\s*=\s*'[^']*'", "'warehouse' = '" + $WarehouseUri + "'")
    $content = [regex]::Replace($content, '(?is)\)\s*PARTITIONED BY\s*\(\s*`k1`\s*\)\s*WITH\s*\(\s*.*?\s*\);', $tableOptions)
    if ($EngineConfig.ContainsKey('UnpartitionedTableOptionsTemplate')) {
        $unpartitionedTableOptions = $EngineConfig.UnpartitionedTableOptionsTemplate.Replace('{METASTORE_URI}', $MetastoreUri).Replace('{WAREHOUSE_URI}', $WarehouseUri)
        $content = [regex]::Replace($content, '(?is)\)\s*WITH\s*\(\s*.*?\s*\);', $unpartitionedTableOptions)
    }

    return $content
}

function Ensure-TestLibDir {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EngineName,

        [Parameter(Mandatory = $true)]
        [string]$DestinationDir
    )

    if ($EngineName -eq 'hudi') {
        return
    }

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

function Get-VerifyRecordsFromOutput {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Output
    )

    $rows = [System.Collections.Generic.List[string]]::new()
    $counts = [System.Collections.Generic.List[string]]::new()

    foreach ($line in ($Output -split "`r?`n")) {
        if ($line -match '^\|\s*((?:row|count)\^[^\|]+?)\s*\|$') {
            $value = $Matches[1].Trim()
            if ($value.StartsWith('row^')) {
                $rows.Add($value)
            }
            elseif ($value.StartsWith('count^')) {
                $counts.Add($value)
            }
        }
    }

    return @{
        Rows = @($rows)
        Counts = @($counts)
    }
}

function Get-SortedContentText {
    param(
        [string[]]$Lines
    )

    return [string]::Join("`n", ($Lines | Sort-Object))
}

function Get-SeedSqlBlocks {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    $firstCreateMatch = [regex]::Match($SqlContent, '(?im)^\s*CREATE TABLE\s+IF\s+NOT\s+EXISTS\s+')
    if (-not $firstCreateMatch.Success) {
        throw 'Failed to find the first CREATE TABLE statement in rendered seed SQL.'
    }

    $preamble = $SqlContent.Substring(0, $firstCreateMatch.Index)
    $matches = [regex]::Matches($SqlContent, '(?is)CREATE TABLE\s+IF\s+NOT\s+EXISTS\s+.*?;\s*INSERT INTO\s+.*?;')
    if ($matches.Count -eq 0) {
        throw 'Failed to split rendered seed SQL into table blocks.'
    }

    $blocks = @()
    foreach ($match in $matches) {
        $tableMatch = [regex]::Match($match.Value, '(?is)CREATE TABLE\s+IF\s+NOT\s+EXISTS\s+([^\s(]+)')
        if (-not $tableMatch.Success) {
            throw 'Failed to parse table name from seed block.'
        }

        $tableIdentifier = $tableMatch.Groups[1].Value
        $tableName = $tableIdentifier.Split('.')[-1]
        $blocks += [pscustomobject]@{
            TableName = $tableName
            Content = $preamble + $match.Value + [Environment]::NewLine
        }
    }

    return $blocks
}

function Ensure-BatchRuntimeMode {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    if ($SqlContent -match "(?im)^\s*SET\s+'execution\.runtime-mode'\s*=") {
        return $SqlContent
    }

    return "SET 'execution.runtime-mode' = 'batch';" + [Environment]::NewLine + [Environment]::NewLine + $SqlContent
}

function Ensure-HudiBatchReadHints {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    $updatedLines = foreach ($line in ($SqlContent -split "`r?`n")) {
        if ($line -match "OPTIONS\('read\.streaming\.enabled'\s*=\s*'false'\)") {
            $line
            continue
        }

        [regex]::Replace(
            $line,
            '(?im)(\bFROM\b|\bJOIN\b)\s+(hudi_(?:dwd|dws|dim)\.[A-Za-z0-9_]+)',
            "`$1 `$2 /*+ OPTIONS('read.streaming.enabled' = 'false') */"
        )
    }

    return [string]::Join([Environment]::NewLine, $updatedLines)
}

function Convert-VerifySqlToSequentialSelects {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    return [regex]::Replace($SqlContent, '(?im)^\s*UNION ALL\s*$', ';')
}

function Remove-ColumnComments {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    return [regex]::Replace($SqlContent, "(?is)\s+COMMENT\s+'[^']*'", '')
}

function Remove-NonAsciiCharacters {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    return [regex]::Replace($SqlContent, "[^\u0009\u000A\u000D\u0020-\u007E]", '')
}

function Normalize-ColumnDefinitionLines {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    $normalizedLines = foreach ($line in ($SqlContent -split "`r?`n")) {
        if ($line -match '^(?<prefix>\s*`[^`]+`\s+)(?<type>STRING|BIGINT|INT|TIMESTAMP\(\d+\)|DECIMAL\(\d+\s*,\s*\d+\))(?<rest>.*)$') {
            $suffix = if ($line.TrimEnd() -match ',\s*$') { ',' } else { '' }
            $Matches['prefix'] + $Matches['type'] + $suffix
            continue
        }

        $line
    }

    return [string]::Join([Environment]::NewLine, $normalizedLines)
}

function Assert-ExpectedTableCounts {
    param(
        [string[]]$CountLines,

        [string[]]$ExpectedTables
    )

    $tableCounts = @{}
    foreach ($countLine in $CountLines) {
        $parts = $countLine.Split('^')
        if ($parts.Count -ne 3) {
            throw "Unexpected count line: $countLine"
        }

        $tableName = $parts[1]
        $countValue = [int]$parts[2]
        $tableCounts[$tableName] = $countValue
    }

    $missingTables = @()
    $emptyTables = @()
    foreach ($tableName in $ExpectedTables) {
        if (-not $tableCounts.ContainsKey($tableName)) {
            $missingTables += $tableName
            continue
        }
        if ($tableCounts[$tableName] -le 0) {
            $emptyTables += $tableName
        }
    }

    if ($missingTables.Count -gt 0) {
        throw "Missing table count rows: $($missingTables -join ', ')"
    }
    if ($emptyTables.Count -gt 0) {
        throw "The following tables are empty after execution: $($emptyTables -join ', ')"
    }
}

function Assert-MatchingContent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,

        [Parameter(Mandatory = $true)]
        [string]$ExpectedText,

        [Parameter(Mandatory = $true)]
        [string]$ActualText
    )

    if ($ExpectedText -eq $ActualText) {
        return
    }

    $expectedLines = if ($ExpectedText) { $ExpectedText -split "`n" } else { @() }
    $actualLines = if ($ActualText) { $ActualText -split "`n" } else { @() }
    $diff = Compare-Object -ReferenceObject $expectedLines -DifferenceObject $actualLines -SyncWindow 0 | Select-Object -First 20
    $diffText = if ($diff) { ($diff | Out-String).Trim() } else { 'No diff lines could be produced.' }
    throw "$Label comparison failed.`n$diffText"
}

function Invoke-EngineFullSuiteTest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EngineName,

        [string]$BaselineRowsText,

        [string]$BaselineCountsText
    )

    $engineConfig = $engineConfigs[$EngineName]
    Ensure-TestLibDir -EngineName $EngineName -DestinationDir $engineConfig.HadoopLibDir

    $engineTargetRoot = Join-Path $targetRoot ("local-" + $EngineName + "-full-suite")
    if (-not (Test-Path -LiteralPath $engineTargetRoot)) {
        New-Item -ItemType Directory -Path $engineTargetRoot -Force | Out-Null
    }

    Write-Output "Starting local Hive Metastore if needed for $EngineName ..."
    Invoke-RepoScript -ScriptPath $startMetastoreScript -Parameters @{
        LibDir = $MetastoreLibDir
    } | Out-Null

    $metastoreUri = Get-LocalMetastoreUri -HiveSitePath $LocalHiveConfFile
    $warehouseUri = 'file:///D:/softDir/hive-local/warehouse'

    Restart-LocalFlinkCluster -EngineName $EngineName -EngineConfig $engineConfig -FlinkHomePath $FlinkHome

    $renderedResetSql = Join-Path $engineTargetRoot 'reset.sql'
    $renderedSeedSql = Join-Path $engineTargetRoot 'seed.sql'
    $renderedVerifySql = Join-Path $engineTargetRoot 'verify.sql'

    Write-Utf8NoBomFile -Path $renderedResetSql -Content (Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $resetSqlFile -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri)
    Write-Utf8NoBomFile -Path $renderedSeedSql -Content (Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $seedSqlFile -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri)
    $renderedVerifySqlContent = Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $verifySqlFile -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri
    $renderedVerifySqlContent = Convert-VerifySqlToSequentialSelects -SqlContent $renderedVerifySqlContent
    Write-Utf8NoBomFile -Path $renderedVerifySql -Content $renderedVerifySqlContent

    $targetSqlFiles = Get-ChildItem -Path $engineConfig.TargetSqlDir -Filter '*.sql' | Sort-Object Name
    if ($targetSqlFiles.Count -ne 21) {
        throw "Expected 21 DWS SQL files for $EngineName, found $($targetSqlFiles.Count) in $($engineConfig.TargetSqlDir)"
    }

    $renderedTargetSqlFiles = @()
    foreach ($targetSqlFile in $targetSqlFiles) {
        $renderedPath = Join-Path $engineTargetRoot $targetSqlFile.Name
        $renderedTargetSql = Get-EngineLocalSql -SqlContent (Get-Content -LiteralPath $targetSqlFile.FullName -Raw) -EngineConfig $engineConfig -MetastoreUri $metastoreUri -WarehouseUri $warehouseUri
        if ($EngineName -eq 'hudi') {
            $renderedTargetSql = Ensure-HudiBatchReadHints -SqlContent $renderedTargetSql
        }
        Write-Utf8NoBomFile -Path $renderedPath -Content (Ensure-BatchRuntimeMode -SqlContent $renderedTargetSql)
        $renderedTargetSqlFiles += $renderedPath
    }

    Write-Output "Dropping local full-suite tables for $EngineName ..."
    Invoke-RepoScript -ScriptPath $runSqlScript -Parameters @{
        SqlFile = $renderedResetSql
        FlinkHome = $FlinkHome
        HadoopLibDir = $engineConfig.HadoopLibDir
        TimeoutSeconds = $TimeoutSeconds
    } | Out-Null

    Write-Output "Removing local full-suite warehouse directories for $EngineName ..."
    foreach ($relativeDir in $engineConfig.WarehouseDirs) {
        Remove-TestWarehouseDirectory -WarehouseRoot $LocalWarehouseRoot -RelativePath $relativeDir
    }

    Write-Output "Seeding source tables for $EngineName ..."
    $seedBlocks = Get-SeedSqlBlocks -SqlContent (Get-Content -LiteralPath $renderedSeedSql -Raw)
    $seedIndex = 0
    foreach ($seedBlock in $seedBlocks) {
        $seedIndex++
        $seedBlockFile = Join-Path $engineTargetRoot ("seed-" + ('{0:D2}' -f $seedIndex) + "-" + $seedBlock.TableName + ".sql")
        $seedBlockLog = Join-Path $engineTargetRoot ("seed-" + ('{0:D2}' -f $seedIndex) + "-" + $seedBlock.TableName + ".log")
        Write-Utf8NoBomFile -Path $seedBlockFile -Content $seedBlock.Content

        $seedParameters = @{
            SqlFile = $seedBlockFile
            FlinkHome = $FlinkHome
            HadoopLibDir = $engineConfig.HadoopLibDir
            TimeoutSeconds = $TimeoutSeconds
            WaitForCompletion = $true
            JobWaitTimeoutSeconds = $TimeoutSeconds
            LogFile = $seedBlockLog
        }
        if ($engineConfig.DisableHiveSync) {
            $seedParameters.DisableHiveSync = $true
        }

        Write-Output ("Seeding " + $seedBlock.TableName + ' for ' + $EngineName + ' ...')
        Invoke-RepoScript -ScriptPath $runSqlScript -Parameters $seedParameters | Out-Null
    }

    $targetSqlIndex = 0
    foreach ($renderedTargetSqlFile in $renderedTargetSqlFiles) {
        if (($targetSqlIndex % $RestartFlinkEveryTargetSqlCount) -eq 0) {
            Restart-LocalFlinkCluster -EngineName $EngineName -EngineConfig $engineConfig -FlinkHomePath $FlinkHome
        }

        $targetSqlIndex++
        $targetName = Split-Path -Path $renderedTargetSqlFile -Leaf
        Write-Output ("Executing " + $targetName + ' for ' + $EngineName + ' ...')
        $runParameters = @{
            SqlFile = $renderedTargetSqlFile
            PDate = $PDate
            FlinkHome = $FlinkHome
            HadoopLibDir = $engineConfig.HadoopLibDir
            TimeoutSeconds = $TimeoutSeconds
            WaitForCompletion = $true
            JobWaitTimeoutSeconds = $TimeoutSeconds
            LogFile = (Join-Path $engineTargetRoot ("run-" + $targetName + ".log"))
        }
        if ($engineConfig.DisableHiveSync) {
            $runParameters.DisableHiveSync = $true
        }
        Invoke-RepoScript -ScriptPath $runSqlScript -Parameters $runParameters | Out-Null
    }

    Write-Output "Querying verification rows for $EngineName ..."
    Restart-LocalFlinkCluster -EngineName $EngineName -EngineConfig $engineConfig -FlinkHomePath $FlinkHome
    $verifyOutput = Invoke-RepoScript -ScriptPath $runSqlScript -Parameters @{
        SqlFile = $renderedVerifySql
        FlinkHome = $FlinkHome
        HadoopLibDir = $engineConfig.HadoopLibDir
        TimeoutSeconds = $TimeoutSeconds
        LogFile = (Join-Path $engineTargetRoot 'verify.log')
    }

    $records = Get-VerifyRecordsFromOutput -Output $verifyOutput
    if ($records.Rows.Count -eq 0 -or $records.Counts.Count -eq 0) {
        throw "Failed to parse verification output for $EngineName. See $(Join-Path $engineTargetRoot 'verify.log')"
    }

    Assert-ExpectedTableCounts -CountLines $records.Counts -ExpectedTables $expectedTableNames

    $rowsText = Get-SortedContentText -Lines $records.Rows
    $countsText = Get-SortedContentText -Lines $records.Counts

    Write-Utf8NoBomFile -Path (Join-Path $engineTargetRoot 'actual-lines.txt') -Content ($rowsText + "`n")
    Write-Utf8NoBomFile -Path (Join-Path $engineTargetRoot 'table-counts.txt') -Content ($countsText + "`n")

    if ($EngineName -ne 'hudi') {
        if (-not $BaselineRowsText) {
            throw 'Baseline row text is empty.'
        }
        if (-not $BaselineCountsText) {
            throw 'Baseline count text is empty.'
        }

        Assert-MatchingContent -Label ($EngineName + ' actual rows') -ExpectedText $BaselineRowsText -ActualText $rowsText
        Assert-MatchingContent -Label ($EngineName + ' table counts') -ExpectedText $BaselineCountsText -ActualText $countsText
    }

    Write-Output ($EngineName + ' full-suite verification passed.')

    return @{
        RowsText = $rowsText
        CountsText = $countsText
        VerifyOutput = $verifyOutput
        TargetRoot = $engineTargetRoot
    }
}

$baselineRowsText = $null
$baselineCountsText = $null
$baselineRowsFile = Join-Path $targetRoot 'local-hudi-full-suite\actual-lines.txt'
$baselineCountsFile = Join-Path $targetRoot 'local-hudi-full-suite\table-counts.txt'
$reuseBaselineMessage = $null

$enginesToRun = switch ($Engine) {
    'all' { @('hudi', 'paimon', 'iceberg') }
    'hudi' { @('hudi') }
    'paimon' {
        if ((Test-Path -LiteralPath $baselineRowsFile) -and (Test-Path -LiteralPath $baselineCountsFile)) {
            $baselineRowsText = (Get-Content -LiteralPath $baselineRowsFile -Raw).TrimEnd()
            $baselineCountsText = (Get-Content -LiteralPath $baselineCountsFile -Raw).TrimEnd()
            $reuseBaselineMessage = "Using existing Hudi baseline from $baselineRowsFile and $baselineCountsFile"
            @('paimon')
        }
        else {
            @('hudi', 'paimon')
        }
    }
    'iceberg' {
        if ((Test-Path -LiteralPath $baselineRowsFile) -and (Test-Path -LiteralPath $baselineCountsFile)) {
            $baselineRowsText = (Get-Content -LiteralPath $baselineRowsFile -Raw).TrimEnd()
            $baselineCountsText = (Get-Content -LiteralPath $baselineCountsFile -Raw).TrimEnd()
            $reuseBaselineMessage = "Using existing Hudi baseline from $baselineRowsFile and $baselineCountsFile"
            @('iceberg')
        }
        else {
            @('hudi', 'iceberg')
        }
    }
}

if ($reuseBaselineMessage) {
    Write-Output $reuseBaselineMessage
}

foreach ($engineName in $enginesToRun) {
    $result = Invoke-EngineFullSuiteTest -EngineName $engineName -BaselineRowsText $baselineRowsText -BaselineCountsText $baselineCountsText
    if ($engineName -eq 'hudi') {
        $baselineRowsText = $result.RowsText
        $baselineCountsText = $result.CountsText
    }
}

Write-Output 'All requested full-suite tasks finished successfully.'
