$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$FlinkJobsPath = Join-Path $RepoRoot "flink_jobs"
$JarTargetPath = "/tmp/traffic-monitoring.jar"

Write-Host ""
Write-Host "Traffic Monitoring Flink Submit Script"
Write-Host "Repo root: $RepoRoot"
Write-Host ""

Write-Host "Checking Docker..."

docker info | Out-Null

$RunningContainers = docker ps --format "{{.Names}}"

if ($RunningContainers -notcontains "flink-jobmanager") {
  throw "flink-jobmanager container is not running. Start Docker Compose first with: docker compose up -d --build"
}

Write-Host "Building Flink job jar..."

docker run --rm `
  -v "${FlinkJobsPath}:/workspace" `
  -w /workspace `
  maven:3.9.9-eclipse-temurin-17 `
  mvn clean package

$Jar = Get-ChildItem "$FlinkJobsPath\target\*.jar" |
  Where-Object { $_.Name -notlike "original-*" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $Jar) {
  throw "No runnable Flink jar found in flink_jobs\target"
}

Write-Host "Using jar: $($Jar.FullName)"
Write-Host "Copying jar to flink-jobmanager..."

docker cp "$($Jar.FullName)" "flink-jobmanager:$JarTargetPath"

Write-Host "Submitting Flink job..."

docker exec flink-jobmanager /opt/flink/bin/flink run -d `
  -c traffic.TrafficMonitoringJob `
  $JarTargetPath

Write-Host ""
Write-Host "Current Flink jobs:"
docker exec flink-jobmanager /opt/flink/bin/flink list

Write-Host ""
Write-Host "Flink job submitted successfully."