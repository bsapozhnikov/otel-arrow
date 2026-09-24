[CmdletBinding()]
param(
    [ValidateSet('OtelArrow', 'Rsyslog', 'LogstashRaw', 'LogstashJson', 'LogstashOtlp')]
    [string]$Target = 'OtelArrow',

    [ValidateSet('Rfc5424', 'Cef')]
    [string]$Format = 'Rfc5424',

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
$targetConfig = switch ($Target) {
    'OtelArrow' { @{ Port = 5514; Topic = 'syslog-otlp-otel_arrow' } }
    'Rsyslog' { @{ Port = 5515; Topic = 'syslog-raw-rsyslog' } }
    'LogstashRaw' { @{ Port = 5516; Topic = 'syslog-raw-logstash' } }
    'LogstashJson' { @{ Port = 5517; Topic = 'syslog-json-logstash' } }
    'LogstashOtlp' { @{ Port = 5518; Topic = 'syslog-otlp-logstash' } }
}
$port = $targetConfig.Port
$topic = $targetConfig.Topic

try {
    do {
        $sequence++
        $body = if ($Message) {
            $Message
        }
        else {
            "kafka-syslog-e2e-${topic}-$([Guid]::NewGuid().ToString('N'))-$sequence"
        }
        $payload = if ($Format -eq 'Cef') {
            $cefName = $body.Replace('\', '\\').Replace('|', '\|')
            "CEF:0|Security|threatmanager|1.0|100|$cefName|10|src=10.0.0.1 dst=2.1.2.2 spt=1232"
        }
        else {
            $timestamp = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
            "<34>1 $timestamp test-host test-app 123 ID47 - $body"
        }
        $bytes = [Text.Encoding]::UTF8.GetBytes($payload)
        [void]$udp.Send($bytes, $bytes.Length, '127.0.0.1', $port)
        Write-Host "Sent $Format to ${Target}: $body"

        if ($Continuous) {
            Start-Sleep -Milliseconds $delayMilliseconds
        }
    } while ($Continuous)
}
finally {
    $udp.Dispose()
}
