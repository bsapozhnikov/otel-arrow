[CmdletBinding()]
param(
    [switch]$Continuous,

    [ValidateRange(1, 1000)]
    [int]$MessagesPerSecond = 1,

    [string]$Message
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$udp = [Net.Sockets.UdpClient]::new()
$sequence = 0
$delayMilliseconds = [Math]::Max(1, [int](1000 / $MessagesPerSecond))

try {
    do {
        $sequence++
        $body = if ($Message) {
            $Message
        }
        else {
            "kafka-syslog-e2e-$([Guid]::NewGuid().ToString('N'))-$sequence"
        }
        $timestamp = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
        $syslog = "<34>1 $timestamp test-host test-app 123 ID47 - $body"
        $bytes = [Text.Encoding]::UTF8.GetBytes($syslog)
        [void]$udp.Send($bytes, $bytes.Length, '127.0.0.1', 5514)
        Write-Host "Sent: $body"

        if ($Continuous) {
            Start-Sleep -Milliseconds $delayMilliseconds
        }
    } while ($Continuous)
}
finally {
    $udp.Dispose()
}
