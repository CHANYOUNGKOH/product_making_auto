[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TicketFile,
    [string]$ImplementerBranch
)

$ErrorActionPreference = "Stop"
$skipPrompt = $PSBoundParameters.ContainsKey("Confirm") -and $ConfirmPreference -eq "None"

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    $stdoutPath = Join-Path $env:TEMP ("close-ticket-stdout-{0}.log" -f ([guid]::NewGuid().ToString("N")))
    $stderrPath = Join-Path $env:TEMP ("close-ticket-stderr-{0}.log" -f ([guid]::NewGuid().ToString("N")))

    try {
        $process = Start-Process `
            -FilePath $FilePath `
            -ArgumentList $Arguments `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        $stdout = if (Test-Path $stdoutPath) { Get-Content -Raw -Path $stdoutPath } else { "" }
        $stderr = if (Test-Path $stderrPath) { Get-Content -Raw -Path $stderrPath } else { "" }
    }
    finally {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }

    $combined = @()
    if ($stdout) { $combined += $stdout.TrimEnd() }
    if ($stderr) { $combined += $stderr.TrimEnd() }
    $text = ($combined -join [Environment]::NewLine).Trim()

    if (-not $AllowFailure -and $process.ExitCode -ne 0) {
        throw "$FilePath $($Arguments -join ' ') 실패: $text"
    }

    return [pscustomobject]@{
        ExitCode = $process.ExitCode
        Output   = @(
            (($stdout + [Environment]::NewLine + $stderr) -split "`r?`n") |
                Where-Object { $_ -ne "" }
        )
        StdOut   = $stdout
        StdErr   = $stderr
        Text     = $text
    }
}

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    return Invoke-NativeCommand -FilePath "git" -Arguments $Arguments -AllowFailure:$AllowFailure
}

function Resolve-TicketPath {
    param(
        [Parameter(Mandatory = $true)][string]$RepoRoot,
        [Parameter(Mandatory = $true)][string]$InputPath
    )

    if (Test-Path $InputPath) {
        return (Resolve-Path $InputPath).Path
    }

    $candidate = Join-Path $RepoRoot $InputPath
    if (Test-Path $candidate) {
        return (Resolve-Path $candidate).Path
    }

    throw "티켓 파일을 찾을 수 없습니다: $InputPath"
}

function Get-BranchMetadata {
    param(
        [Parameter(Mandatory = $true)][string]$BranchName
    )

    if (-not $BranchName.StartsWith("ticket/")) {
        throw "구현 가지 형식이 올바르지 않습니다: $BranchName"
    }

    $tail = $BranchName.Substring(7)
    $lastDash = $tail.LastIndexOf("-")
    if ($lastDash -lt 1 -or $lastDash -ge ($tail.Length - 1)) {
        throw "구현 가지에서 ticket id / owner 를 분리할 수 없습니다: $BranchName"
    }

    return [pscustomobject]@{
        TicketId = $tail.Substring(0, $lastDash)
        Owner    = $tail.Substring($lastDash + 1)
    }
}

function Test-LocalBranchExists {
    param(
        [Parameter(Mandatory = $true)][string]$BranchName
    )

    $result = Invoke-Git -Arguments @("rev-parse", "--verify", "--quiet", "refs/heads/$BranchName") -AllowFailure
    return $result.ExitCode -eq 0
}

function Resolve-ImplementerBranchFromTicket {
    param(
        [Parameter(Mandatory = $true)][string]$TicketId
    )

    $result = Invoke-Git -Arguments @("for-each-ref", "--format=%(refname:short)", "refs/heads/ticket/$TicketId-*")
    $branches = @($result.Output | ForEach-Object { "$_".Trim() } | Where-Object { $_ })

    if ($branches.Count -eq 0) {
        throw "ticket/$TicketId-* 형식의 구현 가지를 찾지 못했습니다."
    }

    if ($branches.Count -gt 1) {
        throw "구현 가지가 여러 개입니다. -ImplementerBranch 를 직접 지정하세요: $($branches -join ', ')"
    }

    return $branches[0]
}

function Get-WorktreePathForBranch {
    param(
        [Parameter(Mandatory = $true)][string]$BranchName
    )

    $lines = Invoke-Git -Arguments @("worktree", "list", "--porcelain")
    $currentWorktree = $null

    foreach ($line in $lines.Output) {
        $text = "$line"
        if ($text.StartsWith("worktree ")) {
            $currentWorktree = $text.Substring(9).Trim()
            continue
        }

        if ($text -eq "branch refs/heads/$BranchName") {
            return $currentWorktree
        }
    }

    return $null
}

function Remove-ManagedWorktree {
    param(
        [Parameter(Mandatory = $true)][string]$BranchName,
        [Parameter(Mandatory = $true)][string]$FallbackPath
    )

    $path = Get-WorktreePathForBranch -BranchName $BranchName
    if (-not $path) {
        $path = $FallbackPath
    }

    if (-not (Test-Path $path)) {
        return $null
    }

    $result = Invoke-Git -Arguments @("worktree", "remove", $path) -AllowFailure
    if ($result.ExitCode -ne 0) {
        throw "worktree 제거 실패 ($path): $($result.Text)"
    }

    return $path
}

function Ensure-PullRequest {
    param(
        [Parameter(Mandatory = $true)][string]$BranchName
    )

    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw "gh CLI 를 찾지 못했습니다."
    }

    $listResult = Invoke-NativeCommand -FilePath "gh" -Arguments @("pr", "list", "--base", "main", "--head", $BranchName, "--json", "number", "--limit", "1") -AllowFailure
    if ($listResult.ExitCode -ne 0) {
        $listText = $listResult.Text
        throw "gh pr list 실패: $listText"
    }

    $listText = $listResult.Text
    if ($listText -and $listText -notmatch "^\s*\[\s*\]\s*$") {
        return "existing"
    }

    $createResult = Invoke-NativeCommand -FilePath "gh" -Arguments @("pr", "create", "--base", "main", "--head", $BranchName, "--fill") -AllowFailure
    if ($createResult.ExitCode -ne 0) {
        $createText = $createResult.Text
        if ($createText -match "already exists") {
            return "existing"
        }
        throw "gh pr create 실패: $createText"
    }

    return "created"
}

function Confirm-CloseTicket {
    if ($skipPrompt) {
        return
    }

    $answer = Read-Host "리뷰 PASS가 확인됐습니다. main에 합치고 worktree를 정리할까요? [y/N]"
    if ($answer -notmatch "^(?i)y(?:es)?$") {
        throw "사용자 확인이 없어 작업을 중단했습니다."
    }
}

if ([string]::IsNullOrWhiteSpace($TicketFile) -and [string]::IsNullOrWhiteSpace($ImplementerBranch)) {
    throw "-TicketFile 또는 -ImplementerBranch 중 하나는 반드시 지정해야 합니다."
}

if (-not [string]::IsNullOrWhiteSpace($TicketFile) -and -not [string]::IsNullOrWhiteSpace($ImplementerBranch)) {
    throw "-TicketFile 과 -ImplementerBranch 는 동시에 지정할 수 없습니다."
}

$repoRoot = (Invoke-Git -Arguments @("rev-parse", "--show-toplevel")).Text
Set-Location $repoRoot

$ticketId = $null
if (-not [string]::IsNullOrWhiteSpace($TicketFile)) {
    $resolvedTicketFile = Resolve-TicketPath -RepoRoot $repoRoot -InputPath $TicketFile
    $ticketId = [System.IO.Path]::GetFileNameWithoutExtension($resolvedTicketFile).ToLowerInvariant()
    $ImplementerBranch = Resolve-ImplementerBranchFromTicket -TicketId $ticketId
}

if (-not (Test-LocalBranchExists -BranchName $ImplementerBranch)) {
    throw "구현 가지가 없습니다: $ImplementerBranch"
}

$branchInfo = Get-BranchMetadata -BranchName $ImplementerBranch
if (-not $ticketId) {
    $ticketId = $branchInfo.TicketId
}

if ($ticketId -ne $branchInfo.TicketId) {
    throw "티켓 파일과 구현 가지의 ticket id 가 다릅니다: $ticketId / $($branchInfo.TicketId)"
}

$reviewBranch = "review/$ticketId"
if (-not (Test-LocalBranchExists -BranchName $reviewBranch)) {
    throw "리뷰어 가지가 없습니다: $reviewBranch"
}

$reviewMessage = (Invoke-Git -Arguments @("log", "-1", "--pretty=%B", $reviewBranch)).Text
if ($reviewMessage -notmatch "VERDICT=PASS") {
    throw "리뷰어 가지의 최신 커밋에서 VERDICT=PASS 를 찾지 못했습니다: $reviewBranch"
}

$prStatus = Ensure-PullRequest -BranchName $ImplementerBranch
Confirm-CloseTicket

Invoke-Git -Arguments @("checkout", "main") | Out-Null
Invoke-Git -Arguments @("pull", "--ff-only") | Out-Null

$mergeResult = Invoke-Git -Arguments @("merge", "--ff-only", $ImplementerBranch) -AllowFailure
if ($mergeResult.ExitCode -ne 0) {
    throw "git merge --ff-only 실패: $($mergeResult.Text)"
}

$implementerWorktree = Remove-ManagedWorktree `
    -BranchName $ImplementerBranch `
    -FallbackPath (Join-Path $repoRoot ".worktrees\$ticketId-$($branchInfo.Owner)")

$reviewerWorktree = Remove-ManagedWorktree `
    -BranchName $reviewBranch `
    -FallbackPath (Join-Path $repoRoot ".worktrees\$ticketId-reviewer")

$deleteResult = Invoke-Git -Arguments @("branch", "-d", $ImplementerBranch) -AllowFailure
if ($deleteResult.ExitCode -ne 0) {
    throw "구현 가지 삭제 실패: $($deleteResult.Text)"
}

Write-Output "STATUS=MERGED"
Write-Output "TICKET=$ticketId"
Write-Output "IMPLEMENTER_BRANCH=$ImplementerBranch"
Write-Output "REVIEW_BRANCH=$reviewBranch"
Write-Output "PR_STATUS=$prStatus"
if ($implementerWorktree) {
    Write-Output "REMOVED_WORKTREE=$implementerWorktree"
}
if ($reviewerWorktree) {
    Write-Output "REMOVED_WORKTREE=$reviewerWorktree"
}
