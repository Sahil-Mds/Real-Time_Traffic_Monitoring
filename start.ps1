# PowerShell Startup Script for Real-Time Traffic Monitoring Project
# Checks dependencies, launches Docker Desktop (if not running), and starts Docker Compose.
# NOTE: No emoji characters used - they cause parser errors in Windows CMD/PowerShell.

$ErrorActionPreference = "Stop"

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "  Real-Time Traffic Monitoring - Topic B Launcher  " -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$ROOT = $PSScriptRoot
$ComposeFile = Join-Path $ROOT "docker-compose.yml"
$SubmitFlinkScript = Join-Path $ROOT "scripts\submit_flink_job.ps1"
$ApiDir = Join-Path $ROOT "api"
$DataDir = Join-Path $ROOT "data\taxi_log_2008_by_id"

function Get-PythonCommand {
    $pythonFromPath = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonFromPath) {
        return "python"
    }

    $python310 = Join-Path $env:LOCALAPPDATA "Programs\Python\Python310\python.exe"
    if (Test-Path $python310) {
        return $python310
    }

    throw "Python not found. Install Python 3.10+ or add Python to PATH."
}

function Wait-ForKafka {
    Write-Host "Waiting for Kafka to become healthy..." -ForegroundColor Yellow

    $timeoutSeconds = 120
    $elapsedSeconds = 0

    while ($elapsedSeconds -lt $timeoutSeconds) {
        Start-Sleep -Seconds 5
        $elapsedSeconds += 5

        $health = docker inspect --format='{{.State.Health.Status}}' kafka 2>$null

        if ($health -eq "healthy") {
            Write-Host "Kafka is healthy." -ForegroundColor Green
            return
        }

        Write-Host "." -NoNewline -ForegroundColor Yellow
    }

    Write-Host ""
    Write-Warning "Kafka health check timed out. Continuing, but Kafka may need more time."
}

function Create-KafkaTopic {
    param (
        [string]$TopicName,
        [int]$Partitions,
        [int]$ReplicationFactor
    )

    docker exec kafka kafka-topics `
        --bootstrap-server kafka:29092 `
        --create `
        --if-not-exists `
        --topic $TopicName `
        --partitions $Partitions `
        --replication-factor $ReplicationFactor | Out-Null

    Write-Host "Topic ready: $TopicName" -ForegroundColor Green
}

Write-Host ""
Write-Host "[1/7] Checking Docker..." -ForegroundColor Cyan

docker info | Out-Null
Write-Host "Docker is running." -ForegroundColor Green

Write-Host ""
Write-Host "[2/7] Starting Docker Compose services..." -ForegroundColor Cyan

docker compose -f $ComposeFile up -d --build

if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed."
}

Write-Host "Docker services started." -ForegroundColor Green

Write-Host ""
Write-Host "[3/7] Waiting for Kafka..." -ForegroundColor Cyan

Wait-ForKafka

Write-Host ""
Write-Host "[4/7] Creating Kafka topics..." -ForegroundColor Cyan

Create-KafkaTopic -TopicName "taxi-raw-data" -Partitions 3 -ReplicationFactor 1
Create-KafkaTopic -TopicName "taxi-locations" -Partitions 3 -ReplicationFactor 1
Create-KafkaTopic -TopicName "violation-events" -Partitions 3 -ReplicationFactor 1
Create-KafkaTopic -TopicName "fleet-metrics" -Partitions 1 -ReplicationFactor 1
Create-KafkaTopic -TopicName "state-updates" -Partitions 3 -ReplicationFactor 1

Write-Host ""
Write-Host "Current Kafka topics:" -ForegroundColor Cyan
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

Write-Host ""
Write-Host "[5/7] Building and submitting Flink job..." -ForegroundColor Cyan

if (-not (Test-Path $SubmitFlinkScript)) {
    throw "Missing script: $SubmitFlinkScript"
}

& powershell -NoProfile -ExecutionPolicy Bypass -File $SubmitFlinkScript

Write-Host ""
Write-Host "[6/7] Checking FastAPI API container..." -ForegroundColor Cyan

$apiReady = $false
$apiTimeoutSeconds = 60
$apiElapsedSeconds = 0

while ($apiElapsedSeconds -lt $apiTimeoutSeconds) {
    Start-Sleep -Seconds 3
    $apiElapsedSeconds += 3

    try {
        $apiHealth = Invoke-RestMethod -Uri "http://localhost:8000/health" -TimeoutSec 5

        if ($apiHealth.status -eq "ok") {
            $apiReady = $true
            break
        }
    } catch {
        Write-Host "." -NoNewline -ForegroundColor Yellow
    }
}

Write-Host ""

if ($apiReady) {
    Write-Host "API container is healthy at http://localhost:8000" -ForegroundColor Green
} else {
    Write-Warning "API health check timed out. Check logs with: docker compose logs api"
}

Write-Host ""
Write-Host "[7/7] Producer setup..." -ForegroundColor Cyan

if (Test-Path $DataDir) {
    Write-Host "Starting Dockerized producer in a new PowerShell window..." -ForegroundColor Cyan

    Start-Process powershell -ArgumentList `
        "-NoProfile -ExecutionPolicy Bypass -Command `"cd '$ROOT'; docker compose --profile producer run --rm producer; pause`"" `
        -WindowStyle Normal

    Write-Host "Producer started through Docker profile." -ForegroundColor Green
} else {
    Write-Warning "Dataset folder not found: $DataDir"
    Write-Host "Producer was not started." -ForegroundColor Yellow
    Write-Host "Place taxi data here: data\taxi_log_2008_by_id" -ForegroundColor Yellow
}

Start-Sleep -Seconds 2

Write-Host ""
Write-Host "===================================================" -ForegroundColor Green
Write-Host "  STARTUP COMPLETE                                " -ForegroundColor Green
Write-Host "===================================================" -ForegroundColor Green
Write-Host "Kafka UI      : http://localhost:8080" -ForegroundColor Cyan
Write-Host "Flink UI      : http://localhost:8081" -ForegroundColor Cyan
Write-Host "API Server    : http://localhost:8000" -ForegroundColor Cyan
Write-Host "API Docs      : http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "Dashboard     : http://localhost:3000" -ForegroundColor Cyan
Write-Host "Redis         : localhost:6379" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Green
Write-Host ""
Write-Host "To stop Docker services:" -ForegroundColor Yellow
Write-Host "docker compose down" -ForegroundColor Gray
Write-Host ""