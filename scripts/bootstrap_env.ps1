param(
    [ValidateSet("dev", "stage", "prod", "all")]
    [string]$Environment = "all",

    [ValidateSet("none", "init", "plan", "apply", "destroy")]
    [string]$Action = "plan",

    [switch]$ForceTemplateCopy,

    [switch]$SkipTerraform,

    [switch]$AutoApprove,

    [string]$TerraformBinary = "terraform"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Message,
        [scriptblock]$Block
    )
    Write-Host "==> $Message" -ForegroundColor Cyan
    & $Block
}

function Invoke-Terraform {
    param(
        [string]$Binary,
        [string]$WorkingDir,
        [string[]]$Args
    )
    $display = "$Binary -chdir=$WorkingDir $($Args -join ' ')"
    Write-Host "    $display" -ForegroundColor DarkGray
    & $Binary "-chdir=$WorkingDir" @Args
    if ($LASTEXITCODE -ne 0) {
        throw "Terraform command failed with exit code $LASTEXITCODE: $display"
    }
}

$scriptDir = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
$repoRoot = Split-Path -Path $scriptDir -Parent
$envRoot = Join-Path $repoRoot "infra\terraform\envs"

if (-not (Test-Path $envRoot)) {
    throw "Environment root not found: $envRoot"
}

$targetEnvironments = @()
if ($Environment -eq "all") {
    $targetEnvironments = @("dev", "stage", "prod")
}
else {
    $targetEnvironments = @($Environment)
}

if (-not $SkipTerraform) {
    $cmd = Get-Command $TerraformBinary -ErrorAction SilentlyContinue
    if (-not $cmd) {
        throw "Terraform binary '$TerraformBinary' was not found. Install Terraform or use -SkipTerraform."
    }
}

foreach ($envName in $targetEnvironments) {
    $envDir = Join-Path $envRoot $envName
    if (-not (Test-Path $envDir)) {
        throw "Environment directory not found: $envDir"
    }

    Write-Host ""
    Write-Host "### Environment: $envName" -ForegroundColor Yellow

    $backendExample = Join-Path $envDir "backend.tf.example"
    $backendFile = Join-Path $envDir "backend.tf"
    $tfvarsExample = Join-Path $envDir "terraform.tfvars.example"
    $tfvarsFile = Join-Path $envDir "terraform.tfvars"

    Invoke-Step -Message "Preparing backend.tf and terraform.tfvars" -Block {
        if (Test-Path $backendExample) {
            if ($ForceTemplateCopy -or -not (Test-Path $backendFile)) {
                Copy-Item -Path $backendExample -Destination $backendFile -Force
                Write-Host "    backend.tf prepared from backend.tf.example"
            }
            else {
                Write-Host "    backend.tf exists, skipping copy"
            }
        }
        else {
            Write-Host "    backend.tf.example not found; skipping"
        }

        if (Test-Path $tfvarsExample) {
            if ($ForceTemplateCopy -or -not (Test-Path $tfvarsFile)) {
                Copy-Item -Path $tfvarsExample -Destination $tfvarsFile -Force
                Write-Host "    terraform.tfvars prepared from terraform.tfvars.example"
            }
            else {
                Write-Host "    terraform.tfvars exists, skipping copy"
            }
        }
        else {
            Write-Host "    terraform.tfvars.example not found; skipping"
        }
    }

    if ($SkipTerraform -or $Action -eq "none") {
        Write-Host "    Skipping Terraform execution for $envName"
        continue
    }

    Invoke-Step -Message "Terraform init ($envName)" -Block {
        Invoke-Terraform -Binary $TerraformBinary -WorkingDir $envDir -Args @("init", "-input=false")
    }

    if ($Action -eq "init") {
        continue
    }

    if ($Action -eq "plan") {
        Invoke-Step -Message "Terraform plan ($envName)" -Block {
            Invoke-Terraform -Binary $TerraformBinary -WorkingDir $envDir -Args @("plan", "-input=false")
        }
        continue
    }

    if ($Action -eq "apply") {
        $applyArgs = @("apply", "-input=false")
        if ($AutoApprove) {
            $applyArgs += "-auto-approve"
        }
        Invoke-Step -Message "Terraform apply ($envName)" -Block {
            Invoke-Terraform -Binary $TerraformBinary -WorkingDir $envDir -Args $applyArgs
        }
        continue
    }

    if ($Action -eq "destroy") {
        $destroyArgs = @("destroy", "-input=false")
        if ($AutoApprove) {
            $destroyArgs += "-auto-approve"
        }
        Invoke-Step -Message "Terraform destroy ($envName)" -Block {
            Invoke-Terraform -Binary $TerraformBinary -WorkingDir $envDir -Args $destroyArgs
        }
        continue
    }
}

Write-Host ""
Write-Host "Bootstrap completed for: $($targetEnvironments -join ', ')" -ForegroundColor Green

