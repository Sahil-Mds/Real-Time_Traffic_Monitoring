$ErrorActionPreference = "Stop"

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "  Real-Time Traffic Monitoring - Smoke Test        " -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$Failures = New-Object System.Collections.Generic.List[string]

function Pass {
    param ([string]$Message)
    Write-Host "[PASS] $Message" -ForegroundColor Green
}

function Fail {
    param ([string]$Message)
    Write-Host "[FAIL] $Message" -ForegroundColor Red
    $Failures.Add($Message)
}

function Warn {
    param ([string]$Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Test-Docker {
    Write-Host ""
    Write-Host "[1/7] Docker" -ForegroundColor Cyan

    try {
        docker info | Out-Null
        Pass "Docker daemon is running"
    } catch {
        Fail "Docker daemon is not running"
    }
}

function Test-ContainerRunning {
    param ([string]$ContainerName)

    try {
        $running = docker inspect --format='{{.State.Running}}' $ContainerName 2>$null

        if ($running -eq "true") {
            Pass "Container running: $ContainerName"
        } else {
            Fail "Container not running: $ContainerName"
        }
    } catch {
        Fail "Container not found: $ContainerName"
    }
}

function Test-HttpEndpoint {
    param (
        [string]$Name,
        [string]$Url
    )

    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5

        if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
            Pass "$Name reachable at $Url"
        } else {
            Fail "$Name returned status code $($response.StatusCode)"
        }
    } catch {
        Fail "$Name not reachable at $Url"
    }
}

function Test-ApiHealth {
    Write-Host ""
    Write-Host "[4/7] FastAPI health" -ForegroundColor Cyan

    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/health" -TimeoutSec 5

        if ($response.status -eq "ok") {
            Pass "FastAPI health endpoint returned ok"
        } else {
            Fail "FastAPI health endpoint returned unexpected response"
        }
    } catch {
        Fail "FastAPI health endpoint not reachable at http://localhost:8000/health"
    }
}

function Test-KafkaTopics {
    Write-Host ""
    Write-Host "[5/7] Kafka topics" -ForegroundColor Cyan

    $requiredTopics = @(
        "taxi-raw-data",
        "taxi-locations",
        "violation-events",
        "fleet-metrics",
        "state-updates"
    )

    try {
        $topicOutput = docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
        $topics = $topicOutput -split "`n" | ForEach-Object { $_.Trim() }

        foreach ($topic in $requiredTopics) {
            if ($topics -contains $topic) {
                Pass "Kafka topic exists: $topic"
            } else {
                Fail "Kafka topic missing: $topic"
            }
        }
    } catch {
        Fail "Could not list Kafka topics"
    }
}

function Test-Redis {
    Write-Host ""
    Write-Host "[6/7] Redis" -ForegroundColor Cyan

    try {
        $redisPing = docker exec redis redis-cli ping

        if ($redisPing.Trim() -eq "PONG") {
            Pass "Redis responded with PONG"
        } else {
            Fail "Redis did not return PONG"
        }
    } catch {
        Fail "Redis health check failed"
    }
}

function Test-FlinkJob {
    Write-Host ""
    Write-Host "[7/7] Flink job" -ForegroundColor Cyan

    try {
        $overview = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5

        if ($overview.jobs -and $overview.jobs.Count -gt 0) {
            $jobStates = @($overview.jobs | ForEach-Object {
                if ($_.state) {
                    "$($_.state)"
                } elseif ($_.status) {
                    "$($_.status)"
                } else {
                    "UNKNOWN"
                }
            })

            Write-Host "Detected Flink REST job states: $($jobStates -join ', ')" -ForegroundColor Gray

            $runningJobs = @($jobStates | Where-Object {
                $_.Trim().ToUpper() -eq "RUNNING"
            })

            if ($runningJobs.Count -gt 0) {
                Pass "At least one Flink job is running"
                return
            }
        }
    } catch {
        Write-Host "Flink REST API job check failed. Trying Flink CLI fallback..." -ForegroundColor Yellow
    }

    try {
        $cliOutput = docker exec flink-jobmanager /opt/flink/bin/flink list 2>&1
        $cliText = $cliOutput -join "`n"

        if ($cliText -match "\(RUNNING\)") {
            Pass "At least one Flink job is running"
        } else {
            Fail "No running Flink job found"
        }
    } catch {
        Fail "Could not check Flink jobs through REST API or Flink CLI"
    }
}

Test-Docker

Write-Host ""
Write-Host "[2/7] Required containers" -ForegroundColor Cyan

Test-ContainerRunning "kafka"
Test-ContainerRunning "redis"
Test-ContainerRunning "flink-jobmanager"
Test-ContainerRunning "flink-taskmanager"
Test-ContainerRunning "api"
Test-ContainerRunning "dashboard"

Write-Host ""
Write-Host "[3/7] Web UIs" -ForegroundColor Cyan

Test-HttpEndpoint "Kafka UI" "http://localhost:8080"
Test-HttpEndpoint "Flink UI" "http://localhost:8081"
Test-HttpEndpoint "Dashboard" "http://localhost:3000"

Test-ApiHealth
Test-KafkaTopics
Test-Redis
Test-FlinkJob

Write-Host ""
Write-Host "===================================================" -ForegroundColor Cyan

if ($Failures.Count -eq 0) {
    Write-Host "SMOKE TEST PASSED" -ForegroundColor Green
    Write-Host "All required services are reachable." -ForegroundColor Green
    exit 0
}

Write-Host "SMOKE TEST FAILED" -ForegroundColor Red
Write-Host "$($Failures.Count) issue(s) found:" -ForegroundColor Red

foreach ($failure in $Failures) {
    Write-Host " - $failure" -ForegroundColor Red
}

exit 1