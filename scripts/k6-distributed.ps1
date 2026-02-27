param(
    [ValidateSet("install-operator", "uninstall-operator", "run", "status", "logs", "delete")]
    [string]$Mode = "run",

    [string]$Namespace = "radic",
    [string]$TestName = "radic-search-dist",
    [string]$ScriptPath = "deploy/k8s/loadtest/k6-search.js",

    [int]$Parallelism = 4,
    [ValidateSet("post", "none")]
    [string]$Cleanup = "post",
    [string]$K6Image = "grafana/k6:0.49.0",

    [string]$TargetBase = "http://gateway.radic.svc.cluster.local:8080",
    [string]$SearchEndpoint = "/search_http",
    [string]$SearchQuery = "golang",
    [string]$Duration = "2m",
    [int]$Rate = 2000,
    [int]$PreAllocatedVUs = 400,
    [int]$MaxVUs = 2000,
    [string]$RequestTimeout = "15s",
    [switch]$ParseJSON,

    [string]$OperatorManifestUrl = "https://raw.githubusercontent.com/grafana/k6-operator/main/bundle.yaml"
)

$ErrorActionPreference = "Stop"

function Assert-Kubectl() {
    if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
        throw "kubectl not found in PATH."
    }
}

function Exec-OrThrow([string]$Cmd) {
    Write-Host ">> $Cmd"
    Invoke-Expression $Cmd
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $Cmd"
    }
}

function Ensure-Namespace([string]$Ns) {
    $null = kubectl get namespace $Ns 2>$null
    if ($LASTEXITCODE -ne 0) {
        Exec-OrThrow "kubectl create namespace $Ns"
    }
}

function Assert-CRD() {
    $null = kubectl get crd testruns.k6.io 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "k6-operator CRD not found. Run: .\scripts\k6-distributed.ps1 -Mode install-operator"
    }
}

function Install-Operator() {
    Exec-OrThrow "kubectl apply -f `"$OperatorManifestUrl`""
    Write-Host "k6-operator installed/updated."
}

function Uninstall-Operator() {
    Exec-OrThrow "kubectl delete -f `"$OperatorManifestUrl`""
    Write-Host "k6-operator removed."
}

function Apply-TestRun() {
    Assert-CRD
    Ensure-Namespace -Ns $Namespace

    if (-not (Test-Path $ScriptPath)) {
        throw "Script file not found: $ScriptPath"
    }

    $cmName = "$TestName-script"
    Exec-OrThrow "kubectl -n $Namespace create configmap $cmName --from-file=k6-search.js=`"$ScriptPath`" --dry-run=client -o yaml | kubectl apply -f -"

    $parseJsonValue = if ($ParseJSON.IsPresent) { "1" } else { "0" }
    $manifest = @"
apiVersion: k6.io/v1alpha1
kind: TestRun
metadata:
  name: $TestName
  namespace: $Namespace
spec:
  parallelism: $Parallelism
  cleanup: $Cleanup
  script:
    configMap:
      name: $cmName
      file: k6-search.js
  runner:
    image: $K6Image
    env:
    - name: TARGET_BASE
      value: "$TargetBase"
    - name: SEARCH_ENDPOINT
      value: "$SearchEndpoint"
    - name: SEARCH_QUERY
      value: "$SearchQuery"
    - name: DURATION
      value: "$Duration"
    - name: RATE
      value: "$Rate"
    - name: PRE_ALLOCATED_VUS
      value: "$PreAllocatedVUs"
    - name: MAX_VUS
      value: "$MaxVUs"
    - name: REQUEST_TIMEOUT
      value: "$RequestTimeout"
    - name: PARSE_JSON
      value: "$parseJsonValue"
"@

    $manifest | kubectl apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "failed to apply TestRun manifest"
    }

    Write-Host ""
    Write-Host "TestRun submitted."
    Write-Host "Watch progress:"
    Write-Host "  kubectl -n $Namespace get testrun $TestName -w"
    Write-Host ""
    Write-Host "View pods:"
    Write-Host "  kubectl -n $Namespace get pods -l k6_cr=$TestName"
}

function Show-Status() {
    Assert-CRD
    Exec-OrThrow "kubectl -n $Namespace get testrun $TestName -o wide"
    Exec-OrThrow "kubectl -n $Namespace get pods -l k6_cr=$TestName -o wide"
}

function Show-Logs() {
    Exec-OrThrow "kubectl -n $Namespace logs -l k6_cr=$TestName --all-containers=true --tail=200 --prefix=true"
}

function Delete-TestRun() {
    Exec-OrThrow "kubectl -n $Namespace delete testrun $TestName --ignore-not-found=true"
    Exec-OrThrow "kubectl -n $Namespace delete configmap $TestName-script --ignore-not-found=true"
}

Assert-Kubectl

switch ($Mode) {
    "install-operator" { Install-Operator }
    "uninstall-operator" { Uninstall-Operator }
    "run" { Apply-TestRun }
    "status" { Show-Status }
    "logs" { Show-Logs }
    "delete" { Delete-TestRun }
}
