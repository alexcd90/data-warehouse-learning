param(
    [string]$FlinkHome = 'D:\softDir\flink\flink-1.18.1',

    [string]$WslJavaRun = '/mnt/d/softDir/jdk/bin/java.exe',

    [string]$HadoopLibDir = 'D:\softDir\flink\hudi-hms-test-libs-curated',

    [string]$TaskManagerProcessMemory = '3072m',

    [string]$TaskManagerNetworkMemory = '512mb',

    [string]$TaskManagerManagedMemory = '256mb',

    [int]$RestPort = 8081,

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

    $prefix = if ($AsRoot) { "-u root " } else { "" }
    $quotedCommand = '"' + $Command.Replace('"', '\"') + '"'
    $argumentString = "${prefix}bash -lc $quotedCommand"

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

    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $encoding)
}

function Get-FlinkOverview {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    try {
        $output = Invoke-WslBash -Command "curl -sS http://localhost:$Port/overview" | Out-String
        if (-not $output.Trim()) {
            return $null
        }
        return ($output.Trim() | ConvertFrom-Json)
    }
    catch {
        return $null
    }
}

if (-not (Test-Path -LiteralPath $FlinkHome)) {
    throw "Flink home not found: $FlinkHome"
}

if (-not (Test-Path -LiteralPath $HadoopLibDir)) {
    throw "Hadoop lib dir not found: $HadoopLibDir"
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$logDir = Join-Path $repoRoot 'target'
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

$resolvedFlinkHome = (Resolve-Path -LiteralPath $FlinkHome).Path
$resolvedHadoopLibDir = (Resolve-Path -LiteralPath $HadoopLibDir).Path
$wslFlinkHome = Convert-ToWslPath -WindowsPath $resolvedFlinkHome
$wslHadoopLibDir = Convert-ToWslPath -WindowsPath $resolvedHadoopLibDir
$runtimeConfDir = Join-Path $logDir 'flink-local-conf'
$runtimeConfFile = Join-Path $runtimeConfDir 'flink-conf.yaml'
$sourceConfDir = Join-Path $resolvedFlinkHome 'conf'
$sourceConfFile = Join-Path $sourceConfDir 'flink-conf.yaml'

if (-not (Test-Path -LiteralPath $sourceConfFile)) {
    throw "Flink conf file not found: $sourceConfFile"
}

Remove-Item -LiteralPath $runtimeConfDir -Recurse -Force -ErrorAction SilentlyContinue
Copy-Item -LiteralPath $sourceConfDir -Destination $runtimeConfDir -Recurse -Force

$baseConfContent = [System.IO.File]::ReadAllText($sourceConfFile)
$overrideLines = @(
    '',
    '# Local single-node overrides for Hudi integration testing',
    "jobmanager.rpc.address: localhost",
    "rest.address: localhost",
    "rest.bind-address: localhost",
    "taskmanager.host: localhost",
    "taskmanager.bind-host: localhost",
    "parallelism.default: 1",
    "taskmanager.numberOfTaskSlots: 1",
    "taskmanager.memory.process.size: $TaskManagerProcessMemory",
    "taskmanager.memory.managed.size: $TaskManagerManagedMemory",
    "taskmanager.memory.network.min: $TaskManagerNetworkMemory",
    "taskmanager.memory.network.max: $TaskManagerNetworkMemory"
)
Write-Utf8NoBomFile -Path $runtimeConfFile -Content ($baseConfContent.TrimEnd() + [Environment]::NewLine + ($overrideLines -join [Environment]::NewLine) + [Environment]::NewLine)
$wslRuntimeConfDir = Convert-ToWslPath -WindowsPath $runtimeConfDir

Invoke-WslBash -AsRoot -Command "if [ ! -e '/D:' ]; then ln -s /mnt/d '/D:'; fi" | Out-Null

$hadoopClasspath = (Invoke-WslBash -Command "find '$wslHadoopLibDir' -maxdepth 1 -name '*.jar' | sort | paste -sd: -" | Out-String).Trim()
if (-not $hadoopClasspath) {
    throw "Failed to build HADOOP_CLASSPATH from $HadoopLibDir"
}

if ($ForceRestart) {
    try {
        Invoke-WslBash -Command "pkill -f 'org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint|org.apache.flink.runtime.taskexecutor.TaskManagerRunner' || true" | Out-Null
    }
    catch {
    }
    Start-Sleep -Seconds 5
}

$overview = Get-FlinkOverview -Port $RestPort
if ($overview -and $overview.taskmanagers -ge 1 -and $overview.'slots-total' -ge 1 -and -not $ForceRestart) {
    Write-Output "Flink cluster is already ready on WSL localhost:$RestPort"
    return
}

$jmOutLog = Join-Path $logDir 'flink-jm.out.log'
$jmErrLog = Join-Path $logDir 'flink-jm.err.log'
$tmOutLog = Join-Path $logDir 'flink-tm.out.log'
$tmErrLog = Join-Path $logDir 'flink-tm.err.log'
Remove-Item -LiteralPath $jmOutLog,$jmErrLog,$tmOutLog,$tmErrLog -Force -ErrorAction SilentlyContinue

$jobManagerCommand = "export FLINK_CONF_DIR='$wslRuntimeConfDir'; export HADOOP_CLASSPATH='$hadoopClasspath'; env JAVA_RUN='$WslJavaRun' '$wslFlinkHome/bin/jobmanager.sh' start-foreground"
$taskManagerCommand = "export FLINK_CONF_DIR='$wslRuntimeConfDir'; export HADOOP_CLASSPATH='$hadoopClasspath'; env JAVA_RUN='$WslJavaRun' '$wslFlinkHome/bin/taskmanager.sh' start-foreground"
$jobManagerArgs = 'bash -lc "' + $jobManagerCommand.Replace('"', '\"') + '"'
$taskManagerArgs = 'bash -lc "' + $taskManagerCommand.Replace('"', '\"') + '"'

$jobManagerProcess = Start-Process -FilePath 'wsl.exe' `
    -ArgumentList $jobManagerArgs `
    -WindowStyle Hidden `
    -PassThru `
    -RedirectStandardOutput $jmOutLog `
    -RedirectStandardError $jmErrLog

Start-Sleep -Seconds 8

$taskManagerProcess = Start-Process -FilePath 'wsl.exe' `
    -ArgumentList $taskManagerArgs `
    -WindowStyle Hidden `
    -PassThru `
    -RedirectStandardOutput $tmOutLog `
    -RedirectStandardError $tmErrLog

$started = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 3
    $overview = Get-FlinkOverview -Port $RestPort
    if ($overview -and $overview.taskmanagers -ge 1 -and $overview.'slots-total' -ge 1) {
        $started = $true
        break
    }
}

if (-not $started) {
    $jmErrTail = if (Test-Path -LiteralPath $jmErrLog) { Get-Content -LiteralPath $jmErrLog -Tail 80 | Out-String } else { '' }
    $tmErrTail = if (Test-Path -LiteralPath $tmErrLog) { Get-Content -LiteralPath $tmErrLog -Tail 80 | Out-String } else { '' }
    $tmOutTail = if (Test-Path -LiteralPath $tmOutLog) { Get-Content -LiteralPath $tmOutLog -Tail 80 | Out-String } else { '' }
    throw "Flink cluster did not become ready on WSL localhost:$RestPort`n=== JM ERR ===`n$jmErrTail`n=== TM ERR ===`n$tmErrTail`n=== TM OUT ===`n$tmOutTail"
}

Write-Output "Flink cluster is ready on WSL localhost:$RestPort (jobmanager pid=$($jobManagerProcess.Id), taskmanager pid=$($taskManagerProcess.Id))"
