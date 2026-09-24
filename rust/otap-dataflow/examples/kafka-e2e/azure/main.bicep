@description('Name of the existing Log Analytics workspace.')
param workspaceName string

@description('Azure region containing the Log Analytics workspace.')
param location string

@description('Name of the custom Log Analytics table.')
param tableName string = 'OtelArrowRawSyslog_CL'

@description('Name of the direct Data Collection Rule.')
param dataCollectionRuleName string = 'otel-arrow-raw-syslog-dcr'

var streamName = 'Custom-${tableName}'
var columns = [
  {
    name: 'TimeGenerated'
    type: 'datetime'
  }
  {
    name: 'RawMessage'
    type: 'string'
  }
  {
    name: 'Message'
    type: 'string'
  }
  {
    name: 'CefVersion'
    type: 'int'
  }
  {
    name: 'DeviceVendor'
    type: 'string'
  }
  {
    name: 'DeviceProduct'
    type: 'string'
  }
  {
    name: 'DeviceVersion'
    type: 'string'
  }
  {
    name: 'DeviceEventClassId'
    type: 'string'
  }
  {
    name: 'EventName'
    type: 'string'
  }
  {
    name: 'CefSeverity'
    type: 'string'
  }
  {
    name: 'SourceAddress'
    type: 'string'
  }
  {
    name: 'DestinationAddress'
    type: 'string'
  }
  {
    name: 'SourcePort'
    type: 'string'
  }
  {
    name: 'SeverityText'
    type: 'string'
  }
  {
    name: 'SeverityNumber'
    type: 'int'
  }
  {
    name: 'SyslogVersion'
    type: 'int'
  }
  {
    name: 'Facility'
    type: 'int'
  }
  {
    name: 'SyslogSeverity'
    type: 'int'
  }
  {
    name: 'HostName'
    type: 'string'
  }
  {
    name: 'AppName'
    type: 'string'
  }
  {
    name: 'ProcessId'
    type: 'string'
  }
  {
    name: 'ProcessIdNumeric'
    type: 'long'
  }
  {
    name: 'MessageId'
    type: 'string'
  }
  {
    name: 'StructuredData'
    type: 'string'
  }
  {
    name: 'InputFormat'
    type: 'string'
  }
]

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: workspaceName
}

resource table 'Microsoft.OperationalInsights/workspaces/tables@2023-09-01' = {
  parent: workspace
  name: tableName
  properties: {
    schema: {
      name: tableName
      columns: columns
    }
  }
}

resource dataCollectionRule 'Microsoft.Insights/dataCollectionRules@2023-03-11' = {
  name: dataCollectionRuleName
  location: location
  kind: 'Direct'
  properties: {
    streamDeclarations: {
      '${streamName}': {
        columns: columns
      }
    }
    destinations: {
      logAnalytics: [
        {
          name: 'log-analytics'
          workspaceResourceId: workspace.id
        }
      ]
    }
    dataFlows: [
      {
        streams: [
          streamName
        ]
        destinations: [
          'log-analytics'
        ]
        transformKql: 'source'
        outputStream: streamName
      }
    ]
  }
  dependsOn: [
    table
  ]
}

output dataCollectionRuleResourceId string = dataCollectionRule.id
output dcrEndpoint string = dataCollectionRule.properties.endpoints.logsIngestion
output dcrId string = dataCollectionRule.properties.immutableId
output streamName string = streamName
output workspaceId string = workspace.properties.customerId
