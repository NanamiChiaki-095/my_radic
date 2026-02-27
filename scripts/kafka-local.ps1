param(
    [ValidateSet("start", "stop", "status")]
    [string]$Mode = "start",

    [string]$KafkaHome = "D:\Kafka\kafka_2.12-3.5.0",
    [string]$Topic = "radic_doc_topic",
    [int]$Partitions = 3,
    [int]$ReplicationFactor = 1
)

$ErrorActionPreference = "Stop"

$StateDir = Join-Path $PSScriptRoot ".kafka-local"
$LogDir = Join-Path $StateDir "logs"
$KafkaDataDir = Join-Path $StateDir "kafka-logs"
$ZkPidFile = Join-Path $StateDir "zookeeper.pid"
$KafkaPidFile = Join-Path $StateDir "kafka.pid"
$ZkLog = Join-Path $LogDir "zookeeper.log"
$KafkaLog = Join-Path $LogDir "kafka.log"

$ZkPort = 2181
$KafkaPort = 9092

function Ensure-Directory([string]$Path) {
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Is-ProcessAlive([int]$ProcessId) {
    if ($ProcessId -le 0) { return $false }
    try {
        Get-Process -Id $ProcessId -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Read-Pid([string]$PidFile) {
    if (-not (Test-Path $PidFile)) { return 0 }
    $raw = (Get-Content -Path $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $raw) { return 0 }
    $parsedPid = 0
    [void][int]::TryParse($raw, [ref]$parsedPid)
    return $parsedPid
}

function Write-Pid([string]$PidFile, [int]$ProcessId) {
    Set-Content -Path $PidFile -Value $ProcessId -Encoding ASCII
}

function Clear-Pid([string]$PidFile) {
    if (Test-Path $PidFile) {
        Remove-Item -Path $PidFile -Force
    }
}

function Is-PortListening([int]$Port) {
    try {
        return (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop | Measure-Object).Count -gt 0
    } catch {
        return $false
    }
}

function Wait-Port([int]$Port, [int]$TimeoutSec) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Is-PortListening -Port $Port) {
            return $true
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Stop-ManagedProcess([string]$Name, [string]$PidFile) {
    $procId = Read-Pid -PidFile $PidFile
    if ($procId -gt 0 -and (Is-ProcessAlive -ProcessId $procId)) {
        Stop-Process -Id $procId -Force
        Write-Host "[stop] $Name stopped (pid=$procId)"
    } else {
        Write-Host "[stop] $Name not running"
    }
    Clear-Pid -PidFile $PidFile
}

function Start-BatService(
    [string]$Name,
    [string]$BatPath,
    [string]$ConfigPath,
    [string]$LogPath,
    [string]$PidFile,
    [string[]]$ExtraArgs = @()
) {
    if (-not (Test-Path $BatPath)) {
        throw "$Name start script not found: $BatPath"
    }
    if (-not (Test-Path $ConfigPath)) {
        throw "$Name config not found: $ConfigPath"
    }

    $extra = ""
    if ($ExtraArgs -and $ExtraArgs.Count -gt 0) {
        $extra = " " + (($ExtraArgs | ForEach-Object { "`"$_`"" }) -join " ")
    }
    $args = "/c `"`"$BatPath`" `"$ConfigPath`"$extra >> `"$LogPath`" 2>&1`""
    $process = Start-Process `
        -FilePath "cmd.exe" `
        -ArgumentList $args `
        -WorkingDirectory $KafkaHome `
        -PassThru

    Write-Pid -PidFile $PidFile -ProcessId $process.Id
    Write-Host "[start] $Name started (pid=$($process.Id)), log=$LogPath"
}

function Ensure-Java() {
    if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
        throw "java not found in PATH. Install JDK and set JAVA_HOME/PATH first."
    }
}

function Show-Status() {
    $zkPid = Read-Pid -PidFile $ZkPidFile
    $kafkaPid = Read-Pid -PidFile $KafkaPidFile

    Write-Host "KafkaHome : $KafkaHome"
    Write-Host "ZooKeeper: pid=$zkPid alive=$(Is-ProcessAlive -ProcessId $zkPid) port$ZkPort=$(Is-PortListening -Port $ZkPort)"
    Write-Host "Kafka    : pid=$kafkaPid alive=$(Is-ProcessAlive -ProcessId $kafkaPid) port$KafkaPort=$(Is-PortListening -Port $KafkaPort)"
    Write-Host "Logs     : $LogDir"
}

function Ensure-Topic([string]$TopicsBatPath) {
    & $TopicsBatPath --bootstrap-server "127.0.0.1:$KafkaPort" --create --if-not-exists --topic $Topic --partitions $Partitions --replication-factor $ReplicationFactor | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create topic '$Topic'."
    }
    $topics = & $TopicsBatPath --bootstrap-server "127.0.0.1:$KafkaPort" --list
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to list topics from Kafka."
    }
    $exists = $false
    foreach ($item in $topics) {
        if ($item -eq $Topic) {
            $exists = $true
            break
        }
    }
    if (-not $exists) {
        throw "Topic '$Topic' was not found after creation."
    }
    Write-Host "[topic] ensured '$Topic' (partitions=$Partitions, rf=$ReplicationFactor)"
}

Ensure-Directory -Path $StateDir
Ensure-Directory -Path $LogDir
Ensure-Directory -Path $KafkaDataDir

$ZkBat = Join-Path $KafkaHome "bin\windows\zookeeper-server-start.bat"
$KafkaBat = Join-Path $KafkaHome "bin\windows\kafka-server-start.bat"
$TopicsBat = Join-Path $KafkaHome "bin\windows\kafka-topics.bat"
$ZkConfig = Join-Path $KafkaHome "config\zookeeper.properties"
$KafkaConfig = Join-Path $KafkaHome "config\server.properties"

switch ($Mode) {
    "status" {
        Show-Status
        break
    }
    "stop" {
        Stop-ManagedProcess -Name "kafka" -PidFile $KafkaPidFile
        Stop-ManagedProcess -Name "zookeeper" -PidFile $ZkPidFile
        Show-Status
        break
    }
    "start" {
        Ensure-Java

        if (-not (Is-PortListening -Port $ZkPort)) {
            Start-BatService -Name "zookeeper" -BatPath $ZkBat -ConfigPath $ZkConfig -LogPath $ZkLog -PidFile $ZkPidFile
            if (-not (Wait-Port -Port $ZkPort -TimeoutSec 30)) {
                throw "ZooKeeper did not become ready on port $ZkPort. Check $ZkLog"
            }
        } else {
            Write-Host "[start] zookeeper already listening on $ZkPort"
        }

        if (-not (Is-PortListening -Port $KafkaPort)) {
            $kafkaOverrides = @(
                "--override", "log.dirs=$KafkaDataDir",
                "--override", "log.cleaner.enable=false",
                "--override", "offsets.topic.replication.factor=1",
                "--override", "transaction.state.log.replication.factor=1",
                "--override", "transaction.state.log.min.isr=1"
            )
            Start-BatService -Name "kafka" -BatPath $KafkaBat -ConfigPath $KafkaConfig -LogPath $KafkaLog -PidFile $KafkaPidFile -ExtraArgs $kafkaOverrides
            if (-not (Wait-Port -Port $KafkaPort -TimeoutSec 45)) {
                throw "Kafka did not become ready on port $KafkaPort. Check $KafkaLog"
            }
        } else {
            Write-Host "[start] kafka already listening on $KafkaPort"
        }

        Ensure-Topic -TopicsBatPath $TopicsBat
        Show-Status
        break
    }
}
