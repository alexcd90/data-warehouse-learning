param(
    [string]$BaseDir = 'D:\softDir\hive-local',

    [string]$JavaHome = 'D:\softDir\jdk',

    [string]$LibDir = 'D:\softDir\flink\hudi-hms-test-libs',

    [int]$Port = 9083,

    [string]$MetastoreHost,

    [string]$WslHiveConfDir = '/opt/software/apache-hive-3.1.3-bin/conf',

    [string]$WslHadoopConfDir = '/opt/software/hadoop-3.1.3/etc/hadoop',

    [int]$StartupTimeoutSeconds = 90,

    [switch]$ResetMetastoreDb,

    [switch]$ForceRestart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Convert-ToWslPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$WindowsPath
    )

    $fullPath = [System.IO.Path]::GetFullPath($WindowsPath)
    if ($fullPath -notmatch '^[A-Za-z]:\\') {
        throw "Only drive-letter paths are supported: $fullPath"
    }

    $drive = $fullPath.Substring(0, 1).ToLowerInvariant()
    $rest = $fullPath.Substring(2).Replace('\', '/')
    return "/mnt/$drive$rest"
}

function Invoke-WslBash {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command,

        [switch]$AsRoot
    )

    $args = @()
    if ($AsRoot) {
        $args += '-u'
        $args += 'root'
    }
    $quotedCommand = '"' + $Command.Replace('"', '\"') + '"'
    $argumentString = if ($AsRoot) {
        "-u root bash -lc $quotedCommand"
    }
    else {
        "bash -lc $quotedCommand"
    }

    $stdoutFile = [System.IO.Path]::GetTempFileName()
    $stderrFile = [System.IO.Path]::GetTempFileName()
    try {
        $process = Start-Process -FilePath 'wsl.exe' `
            -ArgumentList $argumentString `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile

        $stdout = if (Test-Path -LiteralPath $stdoutFile) { [System.IO.File]::ReadAllText($stdoutFile) } else { '' }
        $stderr = if (Test-Path -LiteralPath $stderrFile) { [System.IO.File]::ReadAllText($stderrFile) } else { '' }

        if ($process.ExitCode -ne 0) {
            throw "WSL command failed with exit code $($process.ExitCode)`n$stdout$stderr"
        }
        if ($stdout.Trim()) {
            Write-Output $stdout.TrimEnd()
        }
    }
    finally {
        Remove-Item -LiteralPath $stdoutFile -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $stderrFile -Force -ErrorAction SilentlyContinue
    }
}

function Write-Utf8NoBomFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    $directory = Split-Path -Parent $Path
    if ($directory -and -not (Test-Path -LiteralPath $directory)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }

    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $encoding)
}

function Get-WslDefaultGateway {
    $stdoutFile = [System.IO.Path]::GetTempFileName()
    $stderrFile = [System.IO.Path]::GetTempFileName()
    try {
        $process = Start-Process -FilePath 'wsl.exe' `
            -ArgumentList 'bash -lc "ip route show default | cut -d'' '' -f3 | head -n 1"' `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile

        if ($process.ExitCode -ne 0) {
            return $null
        }

        $gateway = if (Test-Path -LiteralPath $stdoutFile) {
            [System.IO.File]::ReadAllText($stdoutFile).Trim()
        }
        else {
            $null
        }
        if ($gateway) {
            return $gateway
        }
    }
    catch {
    }
    finally {
        Remove-Item -LiteralPath $stdoutFile -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $stderrFile -Force -ErrorAction SilentlyContinue
    }

    return $null
}

function Test-TcpPort {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostName,

        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(1000)) {
            return $false
        }
        $client.EndConnect($async)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Test-LocalListenPort {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    return [bool](Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
}

$javaExe = Join-Path $JavaHome 'bin\java.exe'
if (-not (Test-Path -LiteralPath $javaExe)) {
    throw "Java executable not found: $javaExe"
}

if (-not (Test-Path -LiteralPath $LibDir)) {
    throw "Lib dir not found: $LibDir"
}

$baseDir = [System.IO.Path]::GetFullPath($BaseDir)
$windowsHiveConfDir = Join-Path $baseDir 'conf'
$windowsHadoopConfDir = Join-Path $baseDir 'hadoop-conf'
$logDir = Join-Path $baseDir 'logs'
$warehouseDir = Join-Path $baseDir 'warehouse'
$tmpDir = Join-Path $baseDir 'tmp'
$metastoreDbDir = Join-Path $baseDir 'metastore_db'
$stdoutLog = Join-Path $logDir 'metastore.out.log'
$stderrLog = Join-Path $logDir 'metastore.err.log'

@($windowsHiveConfDir, $windowsHadoopConfDir, $logDir, $warehouseDir, $tmpDir) | ForEach-Object {
    if (-not (Test-Path -LiteralPath $_)) {
        New-Item -ItemType Directory -Path $_ -Force | Out-Null
    }
}

if (Test-Path -LiteralPath $metastoreDbDir) {
    if ($ResetMetastoreDb) {
        Remove-Item -LiteralPath $metastoreDbDir -Recurse -Force
    }
    $serviceProperties = Join-Path $metastoreDbDir 'service.properties'
    if ((Test-Path -LiteralPath $metastoreDbDir) -and -not (Test-Path -LiteralPath $serviceProperties)) {
        Remove-Item -LiteralPath $metastoreDbDir -Recurse -Force
    }
}

$windowsWarehouseUri = 'file:///' + ($warehouseDir.Replace('\', '/'))
$windowsMetastoreDbUrl = 'jdbc:derby:;databaseName=' + ($metastoreDbDir.Replace('\', '/')) + ';create=true'
$resolvedMetastoreHost = if ($MetastoreHost) { $MetastoreHost } else { Get-WslDefaultGateway }
if (-not $resolvedMetastoreHost) {
    $resolvedMetastoreHost = '127.0.0.1'
}

$hiveSite = @"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${resolvedMetastoreHost}:$Port</value>
  </property>
  <property>
    <name>metastore.thrift.uris</name>
    <value>thrift://${resolvedMetastoreHost}:$Port</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>$windowsMetastoreDbUrl</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mine</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateTables</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateColumns</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateConstraints</name>
    <value>true</value>
  </property>
  <property>
    <name>datanucleus.fixedDatastore</name>
    <value>false</value>
  </property>
  <property>
    <name>datanucleus.autoStartMechanismMode</name>
    <value>checked</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>$windowsWarehouseUri</value>
  </property>
  <property>
    <name>metastore.warehouse.dir</name>
    <value>$windowsWarehouseUri</value>
  </property>
  <property>
    <name>metastore.stats.autogather</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.stats.autogather</name>
    <value>false</value>
  </property>
  <property>
    <name>metastore.stats.auto.analyze</name>
    <value>none</value>
  </property>
  <property>
    <name>hive.metastore.stats.auto.analyze</name>
    <value>none</value>
  </property>
</configuration>
"@

$coreSite = @"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>file:///</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>$($tmpDir.Replace('\', '/'))</value>
  </property>
</configuration>
"@

$hdfsSite = @"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
</configuration>
"@

Write-Utf8NoBomFile -Path (Join-Path $windowsHiveConfDir 'hive-site.xml') -Content $hiveSite
Write-Utf8NoBomFile -Path (Join-Path $windowsHiveConfDir 'metastore-site.xml') -Content $hiveSite
Write-Utf8NoBomFile -Path (Join-Path $windowsHadoopConfDir 'core-site.xml') -Content $coreSite
Write-Utf8NoBomFile -Path (Join-Path $windowsHadoopConfDir 'hdfs-site.xml') -Content $hdfsSite

$wslWindowsHiveConfDir = Convert-ToWslPath -WindowsPath $windowsHiveConfDir
$wslWindowsHadoopConfDir = Convert-ToWslPath -WindowsPath $windowsHadoopConfDir

$syncCommand = @"
mkdir -p '$WslHiveConfDir'
mkdir -p '$WslHadoopConfDir'
cp '$wslWindowsHiveConfDir/hive-site.xml' '$WslHiveConfDir/hive-site.xml'
cp '$wslWindowsHiveConfDir/metastore-site.xml' '$WslHiveConfDir/metastore-site.xml'
cp '$wslWindowsHadoopConfDir/core-site.xml' '$WslHadoopConfDir/core-site.xml'
cp '$wslWindowsHadoopConfDir/hdfs-site.xml' '$WslHadoopConfDir/hdfs-site.xml'
"@
Invoke-WslBash -AsRoot -Command $syncCommand

$existingProcesses = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq 'java.exe' -and $_.CommandLine -like '*org.apache.hadoop.hive.metastore.HiveMetaStore*'
}

if ($existingProcesses) {
    if (-not $ForceRestart) {
        if (Test-LocalListenPort -Port $Port) {
            Write-Output "Hive Metastore is already listening on 127.0.0.1:$Port"
            return
        }
    }

    $existingProcesses | ForEach-Object {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}

$env:HADOOP_CONF_DIR = $windowsHadoopConfDir
$env:HIVE_CONF_DIR = $windowsHiveConfDir
$env:HADOOP_HOME = $baseDir

if (Test-Path -LiteralPath $stdoutLog) {
    Remove-Item -LiteralPath $stdoutLog -Force
}
if (Test-Path -LiteralPath $stderrLog) {
    Remove-Item -LiteralPath $stderrLog -Force
}

$process = Start-Process -FilePath $javaExe `
    -ArgumentList @('-Xms256m', '-Xmx512m', '-cp', (Join-Path $LibDir '*'), 'org.apache.hadoop.hive.metastore.HiveMetaStore', '-p', "$Port") `
    -WorkingDirectory $baseDir `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -PassThru

$started = $false
for ($i = 0; $i -lt $StartupTimeoutSeconds; $i++) {
    Start-Sleep -Seconds 1
    if ($process.HasExited) {
        break
    }
    if (Test-LocalListenPort -Port $Port) {
        $started = $true
        break
    }
}

if (-not $started) {
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }

    if (Test-Path -LiteralPath $stderrLog) {
        Get-Content -Path $stderrLog -Tail 120 | Write-Output
    }
    throw "Failed to start Hive Metastore on 127.0.0.1:$Port"
}

Write-Output "Hive Metastore started on 127.0.0.1:$Port (PID=$($process.Id))"
Write-Output "Metastore URI for WSL/Flink: thrift://${resolvedMetastoreHost}:$Port"
Write-Output "Hive conf: $windowsHiveConfDir"
Write-Output "Hadoop conf: $windowsHadoopConfDir"
Write-Output "Stdout log: $stdoutLog"
Write-Output "Stderr log: $stderrLog"
