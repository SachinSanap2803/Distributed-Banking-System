# run_demo.ps1
$dir = $PSScriptRoot
if (-not $dir) { $dir = Get-Location }
Set-Location $dir

# Clean up any old java processes
Get-Process -Name java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

Write-Host ">>> Starting Bank Node 1 (Downtown Branch)..."
$p1 = Start-Process java -ArgumentList @("-cp", ".", "BankNode", "1", "Downtown Branch", "1099") -WorkingDirectory $dir -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 2

Write-Host ">>> Starting Bank Node 2 (Uptown Branch)..."
$p2 = Start-Process java -ArgumentList @("-cp", ".", "BankNode", "2", "Uptown Branch", "1099") -WorkingDirectory $dir -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 2

Write-Host ">>> Starting Bank Node 3 (Metro Branch - Coordinator)..."
$p3 = Start-Process java -ArgumentList @("-cp", ".", "BankNode", "3", "Metro Branch", "1099") -WorkingDirectory $dir -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 3

Write-Host "`n>>> Running ClientApp with User Session Workflow..."
$cmdFile = "$dir\demo_input.txt"
@(
    "1", "ACC-101",                       # Check Alice balance
    "2", "ACC-101", "250.00",             # Deposit $250
    "4", "ACC-101", "ACC-201", "150.00",  # Cross-branch transfer $150 from ACC-101 to ACC-201
    "7",                                  # Show Cluster Status & Leader
    "6",                                  # Show Global Synchronized Audit Log
    "0"                                   # Exit
) | Set-Content $cmdFile

Get-Content $cmdFile | java -cp . ClientApp 1 1099

Remove-Item $cmdFile -ErrorAction SilentlyContinue

Write-Host "`n>>> Stopping Node processes..."
Stop-Process -Id $p1.Id, $p2.Id, $p3.Id -Force -ErrorAction SilentlyContinue
Write-Host ">>> Session Complete!"
