param(
    [Parameter(Mandatory = $true)]
    [string]$SqlFile,

    [string]$FlinkHome = 'D:\softDir\flink\flink-1.18.1',

    [string]$WslJavaRun = '/mnt/d/softDir/jdk/bin/java.exe',

    [string]$PDate,

    [string]$HadoopLibDir,

    [int]$TimeoutSeconds = 0,

    [string]$LogFile,

    [switch]$DisableHiveSync,

    [switch]$WaitForCompletion,

    [int]$JobWaitTimeoutSeconds = 300,

    [int]$RestPort = 8081,

    [switch]$PreparePlaceholderConf
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
            if ($process.ExitCode -eq 124) {
                throw "WSL command timed out after waiting for the SQL client to finish"
            }
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

    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $encoding)
}

function Disable-HudiHiveSyncInSql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlContent
    )

    $lineBreak = if ($SqlContent.Contains("`r`n")) { "`r`n" } else { "`n" }
    $rawLines = [System.Text.RegularExpressions.Regex]::Split($SqlContent, "`r?`n")
    $rewrittenLines = [System.Collections.Generic.List[string]]::new()

    foreach ($line in $rawLines) {
        if ($line -match "(?i)^\s*'hive_sync\.[^']*'\s*=\s*'[^']*'\s*,?\s*$") {
            continue
        }

        $rewrittenLines.Add($line)

        if ($line -match "^(?<indent>\s*)'connector'\s*=\s*'hudi'\s*,?\s*$") {
            $rewrittenLines.Add("$($Matches.indent)'hive_sync.enabled' = 'false',")
        }
    }

    for ($i = 0; $i -lt ($rewrittenLines.Count - 1); $i++) {
        $hasTrailingComma = $rewrittenLines[$i] -match '^(?<prefix>.*),\s*$'
        $lineWithoutTrailingComma = if ($hasTrailingComma) { $Matches.prefix } else { $null }
        $nextLineClosesBlock = $rewrittenLines[$i + 1] -match '^\s*\)'

        if ($hasTrailingComma -and $nextLineClosesBlock) {
            $rewrittenLines[$i] = $lineWithoutTrailingComma
        }
    }

    return [string]::Join($lineBreak, $rewrittenLines)
}

function Get-FlinkJobInfo {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JobId,

        [Parameter(Mandatory = $true)]
        [int]$RestPort
    )

    $command = @"
python3 - <<'PY'
import json
import urllib.request

job_id = '$JobId'
url = f'http://localhost:$RestPort/jobs/{job_id}'
with urllib.request.urlopen(url, timeout=10) as response:
    data = json.load(response)
print(json.dumps(data))
PY
"@

    $output = Invoke-WslBash -Command $command | Out-String
    if (-not $output.Trim()) {
        throw "Empty response returned for Flink job $JobId"
    }

    return ($output.Trim() | ConvertFrom-Json)
}

function Get-FlinkJobFailureDetails {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JobId,

        [Parameter(Mandatory = $true)]
        [int]$RestPort
    )

    $command = @"
python3 - <<'PY'
import json
import urllib.request

job_id = '$JobId'
url = f'http://localhost:$RestPort/jobs/{job_id}/exceptions'
with urllib.request.urlopen(url, timeout=10) as response:
    data = json.load(response)
print(json.dumps(data))
PY
"@

    $output = Invoke-WslBash -Command $command | Out-String
    if (-not $output.Trim()) {
        return $null
    }

    return ($output.Trim() | ConvertFrom-Json)
}

function Wait-FlinkJobsToFinish {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$JobIds,

        [Parameter(Mandatory = $true)]
        [int]$RestPort,

        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $pendingJobIds = [System.Collections.Generic.List[string]]::new()
    foreach ($jobId in ($JobIds | Select-Object -Unique)) {
        $pendingJobIds.Add($jobId)
    }

    while ($pendingJobIds.Count -gt 0) {
        if ((Get-Date) -gt $deadline) {
            throw "Timed out waiting for Flink jobs to finish: $($pendingJobIds -join ', ')"
        }

        for ($index = $pendingJobIds.Count - 1; $index -ge 0; $index--) {
            $jobId = $pendingJobIds[$index]
            $jobInfo = Get-FlinkJobInfo -JobId $jobId -RestPort $RestPort
            $jobState = [string]$jobInfo.state

            switch ($jobState) {
                'FINISHED' {
                    $pendingJobIds.RemoveAt($index)
                }
                'FAILED' {
                    $failureDetails = Get-FlinkJobFailureDetails -JobId $jobId -RestPort $RestPort
                    $rootException = if ($failureDetails -and $failureDetails.'root-exception') {
                        [string]$failureDetails.'root-exception'
                    }
                    else {
                        'No root exception returned by Flink REST API.'
                    }
                    throw "Flink job $jobId failed.`n$rootException"
                }
                'CANCELED' {
                    throw "Flink job $jobId was canceled."
                }
            }
        }

        if ($pendingJobIds.Count -gt 0) {
            Start-Sleep -Seconds 3
        }
    }
}

if (-not (Test-Path -LiteralPath $SqlFile)) {
    throw "SQL file not found: $SqlFile"
}

if (-not (Test-Path -LiteralPath $FlinkHome)) {
    throw "Flink home not found: $FlinkHome"
}

if ($HadoopLibDir -and -not (Test-Path -LiteralPath $HadoopLibDir)) {
    throw "Hadoop lib dir not found: $HadoopLibDir"
}

$resolvedSqlFile = (Resolve-Path -LiteralPath $SqlFile).Path
$resolvedFlinkHome = (Resolve-Path -LiteralPath $FlinkHome).Path
$resolvedHadoopLibDir = if ($HadoopLibDir) { (Resolve-Path -LiteralPath $HadoopLibDir).Path } else { $null }
$resolvedLogFile = $null
$renderedSqlFile = $null

if ($LogFile) {
    $logParent = Split-Path -Path $LogFile -Parent
    if ($logParent -and -not (Test-Path -LiteralPath $logParent)) {
        New-Item -ItemType Directory -Path $logParent -Force | Out-Null
    }
    $resolvedLogFile = [System.IO.Path]::GetFullPath($LogFile)
}

if ($PreparePlaceholderConf) {
    $prepareCommand = @'
mkdir -p /opt/software/apache-hive-3.1.3-bin/conf
mkdir -p /opt/software/hadoop-3.1.3/etc/hadoop

if [ ! -f /opt/software/apache-hive-3.1.3-bin/conf/hive-site.xml ]; then
cat > /opt/software/apache-hive-3.1.3-bin/conf/hive-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://192.168.244.129:9083</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
</configuration>
EOF
fi

if [ ! -f /opt/software/hadoop-3.1.3/etc/hadoop/core-site.xml ]; then
cat > /opt/software/hadoop-3.1.3/etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://192.168.244.129:8020</value>
  </property>
</configuration>
EOF
fi

if [ ! -f /opt/software/hadoop-3.1.3/etc/hadoop/hdfs-site.xml ]; then
cat > /opt/software/hadoop-3.1.3/etc/hadoop/hdfs-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
</configuration>
EOF
fi
'@

    Invoke-WslBash -AsRoot -Command $prepareCommand
}

if ($PDate -or $DisableHiveSync) {
    $sqlContent = [System.IO.File]::ReadAllText($resolvedSqlFile)
    $renderedContent = $sqlContent

    if ($PDate) {
        if ($PDate -notmatch '^\d{4}-\d{2}-\d{2}$') {
            throw "PDate must be in yyyy-MM-dd format: $PDate"
        }
        $renderedContent = $renderedContent.Replace('${pdate}', $PDate)
    }

    if ($DisableHiveSync) {
        $renderedContent = Disable-HudiHiveSyncInSql -SqlContent $renderedContent
    }

    $renderedSqlFile = Join-Path ([System.IO.Path]::GetTempPath()) ("flink-sql-" + [System.Guid]::NewGuid().ToString('N') + ".sql")
    Write-Utf8NoBomFile -Path $renderedSqlFile -Content $renderedContent
}

$sqlFileForExecution = if ($renderedSqlFile) { $renderedSqlFile } else { $resolvedSqlFile }

$wslSqlFile = Convert-ToWslPath -WindowsPath $sqlFileForExecution
$wslFlinkHome = Convert-ToWslPath -WindowsPath $resolvedFlinkHome
$wslHadoopLibDir = if ($resolvedHadoopLibDir) { Convert-ToWslPath -WindowsPath $resolvedHadoopLibDir } else { $null }
$wslLogFile = if ($resolvedLogFile) { Convert-ToWslPath -WindowsPath $resolvedLogFile } else { $null }

$commandParts = [System.Collections.Generic.List[string]]::new()
$commandParts.Add("set -o pipefail")

if ($wslHadoopLibDir) {
    $commandParts.Add("HADOOP_CLASSPATH=`$(find '$wslHadoopLibDir' -maxdepth 1 -name '*.jar' | sort | paste -sd: -)")
    $commandParts.Add("export HADOOP_CLASSPATH")
}

$sqlClientCommand = "env JAVA_RUN='$WslJavaRun' '$wslFlinkHome/bin/sql-client.sh' embedded -f '$wslSqlFile'"
if ($TimeoutSeconds -gt 0) {
    $sqlClientCommand = "timeout ${TimeoutSeconds}s $sqlClientCommand"
}

if ($wslLogFile) {
    $commandParts.Add("$sqlClientCommand 2>&1 | tee '$wslLogFile'")
    $commandParts.Add("exit `${PIPESTATUS[0]}")
}
else {
    $commandParts.Add($sqlClientCommand)
}

$runCommand = [string]::Join('; ', $commandParts)
try {
    $runOutput = Invoke-WslBash -Command $runCommand | Out-String
    $normalizedOutput = $runOutput -replace "`e\[[\d;]*m", ''
    if ($normalizedOutput -match '\[ERROR\]') {
        throw "Flink SQL client reported an error.`n$runOutput"
    }
    if ($WaitForCompletion) {
        $jobIdMatches = [System.Text.RegularExpressions.Regex]::Matches($normalizedOutput, 'Job ID:\s*([0-9a-fA-F]+)')
        if ($jobIdMatches.Count -gt 0) {
            $jobIds = foreach ($match in $jobIdMatches) { $match.Groups[1].Value }
            Wait-FlinkJobsToFinish -JobIds $jobIds -RestPort $RestPort -TimeoutSeconds $JobWaitTimeoutSeconds
        }
    }
    if ($runOutput.Trim()) {
        Write-Output $runOutput.TrimEnd()
    }
}
finally {
    if ($renderedSqlFile -and (Test-Path -LiteralPath $renderedSqlFile)) {
        Remove-Item -LiteralPath $renderedSqlFile -Force
    }
}
