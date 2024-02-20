<#
    .SYNOPSIS
        This runbook is intended to help customers migrate their machines and schedules onboarded to the legacy Azure Update Management using Automation solution to the latest Azure Update Manager solution.
        This script will enable periodic assessment for machines and migrate software update configurations to MRP maintenance configurations.

    .DESCRIPTION
        This runbook will do one of the below based on the parameters provided to it.
        The runbook will be using the User-Assigned Managed Identity whose client id is passed as a parameter for authenticating ARM calls. Please ensure the managed idenity is assigned the proper role definitions before executing this runbook.
        Non-Azure machines which are not onboarded to Arc will not be onboarded to Azure Update Manager.
        1. If EnablePeriodicAssessmentForMachinesOnboardedToUpdateManagement parameter is true, it will enable periodic assessment for all azure/arc machines onboarded to Automation Update Management under the automation account where the runbook is executing.
        2. if MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines parameters is true,
            2.1. It will enable periodic asssessment for all azure/arc machines either attached or picked up through azure dynamic queries of software update configurations under the automation account where the runbook is executing.
            2.2. It will set required patch properties for scheduled patching for all azure machines either attached or picked up through dynamic queries of software update configurations under the automation account where the runbook is executing.
            2.3. It will migrate software update configurations by creating equivalent MRP maintenance configurations. Maintenance configurations will be created in the region where the automation account resides and in the resource group provided as input.
                2.3.1. Pre/Post tasks of software update configurations will not be migrated.
                2.3.2. Saved search queries of software update configurations will not be migrated.

    .PARAMETER AutomationAccountResourceId
        Mandatory
        Automation Account Resource Id.

    .PARAMETER UserManagedServiceIdentityClientId
        Mandatory
        Client Id of the User Assigned Managed Idenitity.
    
    .PARAMETER EnablePeriodicAssessmentForMachinesOnboardedToUpdateManagement
        Optional
        
    .PARAMETER MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines
        Optional
        
    .PARAMETER ResourceGroupNameForMaintenanceConfigurations
        Optional
        Please provide resource group name when migrating software update configurations.
        The resource group name should not be more than 36 characters.
        The resource group name which will be used for creating a resource group in the same region as the automation account. The maintenance configurations for the migrated software update configurations from the automation account will be residing here.
    
    .EXAMPLE
        Migration -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"  -ClientId "########-####-####-####-############" -EnablePeriodicAssessment $true MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines $true

    .OUTPUTS
        Outputs the status of machines and software update configurations post migration to the output stream.
#>
param(

    [Parameter(Mandatory = $true)]
    [String]$AutomationAccountResourceId,

    [Parameter(Mandatory = $true)]
    [String]$UserManagedServiceIdentityClientId,

    [Parameter(Mandatory = $false)]
    [bool]$EnablePeriodicAssessmentForMachinesOnboardedToUpdateManagement=$true,

    [Parameter(Mandatory = $false)]
    [bool]$MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines=$true,

    [Parameter(Mandatory = $false)]
    [String]$ResourceGroupNameForMaintenanceConfigurations
)

# Telemetry level.
$Debug = "Debug"
$Verbose = "Verbose" 
$Informational = "Informational"
$Warning = "Warning" 
$ErrorLvl = "Error"

# Supported machine resource types.
$AzureVM = "AzureVM"
$ArcServer = "ArcServer"
$NonAzureMachine = "NonAzureMachine"

# Supported OS types.
$Windows = "Windows"
$Linux = "Linux"

# Patch settings status.
$NotAttempted = "NotAttempted"
$Succeeded = "Succeeded"
$Failed = "Failed"

# Supported in Azure Update Manager.
$NotEvaluated = "NotEvaluated"
$Supported = "Supported"
$NotSupported = "NotSupported"

# Software update configuration provisioning state.
$SoftwareUpdateConfigurationSucceededProvisioningState = "Succeeded"

# SUC to MRP reboot settings mapping.
$SoftwareUpdateConfigurationToMaintenanceConfigurationRebootSetting = @{
    "IfRequired" = "IfRequired";
    "Never" = "NeverReboot";
    "Always" = "AlwaysReboot";
}

$RebootOnly = "RebootOnly"

# Configuration frequency settings.
$OneTime = "OneTime"
$Hour = "Hour"
$Hours = "Hours"
$Day = "Day"
$Days = "Days"
$Week = "Week"
$Weeks = "Weeks"
$Month = "Month"
$Months = "Months"

# Configuration Assignment Tag.
$MigrationTag = "AUMMig"

# MRP schedule configurations.
$MRPScheduleMaxAllowedDuration = "04:00"
$MRPScheduleMinAllowedDuration = "01:30"
$MRPScheduleDateFormat = "yyyy-MM-dd HH:mm"

# SUC to MRP reboot settings mapping.
$SoftwareUpdateConfigurationToMaintenanceConfigurationPatchDaySetting = @{
    "1" = "First";
    "2" = "Second";
    "3" = "Third";
    "4" = "Fourth";
    "-1" = "Last"
}

# Dynamic scope global level.
$MaintenanceConfigurationDynamicAssignmentGlobalScope = "global"

# Master runbook name
$MasterRunbookName = "Patch-MicrosoftOMSComputers"

# Software update configuration migration status.
$NotMigrated = "NotMigrated"
$MigrationFailed = "MigrationFailed"
$Migrated = "Migrated"
$PartiallyMigrated = "PartiallyMigrated"

# ARM resource providers.
$ResourceGraphRP = "Microsoft.ResourceGraph";

# ARM resource types.
$VMResourceType = "virtualMachines";
$ArcVMResourceType = "machines";
$ArgResourcesResourceType = "resources";

# API versions.
$VmApiVersion = "2023-03-01";
$ArcVmApiVersion = "2022-12-27";
$AutomationApiVersion = "2022-08-08"
$SoftwareUpdateConfigurationApiVersion = "2023-11-01";
$AutomationAccountApiVersion = "2023-11-01";
$ResourceGraphApiVersion = "2021-03-01";
$MaintenanceConfigurationpApiVersion = "2023-04-01";
$ResourceGroupApiVersion = "2021-04-01";

# HTTP methods.
$GET = "GET"
$PATCH = "PATCH"
$PUT = "PUT"
$POST = "POST"

# ARM endpoints.
$LinkedWorkspacePath = "{0}/linkedWorkspace"
$SoftwareUpdateConfigurationsPath = "{0}/softwareUpdateConfigurations?`$skip={1}"
$AutomationSchedulesPath = "{0}/Schedules/{1}"
$JobSchedulesWithPatchRunbookFilterPath = "{0}/JobSchedules/?$filter=properties/runbook/name%20eq%20'Patch-MicrosoftOMSComputers'&`$skip={1}"
$MaintenanceConfigurationAssignmentPath = "{0}/providers/Microsoft.Maintenance/configurationAssignments/{1}"
$ResourceGroupPath = "{0}/resourceGroups/{1}"

# Error codes.
$Unknown = "Unknown"
$UnhandledException = "UnhandledException"
$NotOnboardedToArcErrorCode = "NotOnboardedToArc"
$NotFoundErrorCode = "404"

# Error messages.
$NotOnboardedToArcErrorMessage = "The machine is not onboarded to ARC. Onboard your machine to ARC to migrate it to AUM, more details here: https://aka.ms/OnboardToArc"
$SoftwareUpdateConfigurationNotProvisionedSuccessfully = "The software update configuration is not provisioned successfully."
$SoftwareUpdateConfigurationInErroredState = "The software update configuration is in errored state."
$FailedToCreateMaintenanceConfiguration = "Failed to create maintenance configuration."
$ExpiredSoftwareUpdateConfigurationMessage = "The software update configuration is already expired."
$DisabledSoftwareUpdateConfigurationMessage = "The associated schedule for the software update configuration is disabled."
$ConfigurationAssignmentFailedForMachines = "Configuration assignment failed for few machines."
$FailedToResolveDynamicAzureQueries = "Failed to resolve few dynamic azure queries."
$FailedToAssignDynamicAzureQueries = "Failed to assign dynamic scopes."
$SoftwareUpdateConfigurationHasSavedSearchQueries = "The software update configuration has saved search queries for non-azure machines. Please create equivalent dynamic scope manually."
$SoftwareUpdateConfigurationHasPrePostTasks = "The software update configuration has pre/post tasks which is not supported in Azure Update Manager."
$SoftwareUpdateConfigurationHasRebootOnlySetting = "The software update configuration has reboot only as the reboot option which is not supported in Azure Update Manager."

# ARG constants.
$ScopeRgRegExPattern = "/subscriptions/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}/resourceGroups/[^/]*[/]?$";
$ScopeSubscriptionRegExPattern = "/subscriptions/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}[/]?$";
$TagFilterClause = "tags[tolower(\""{0}\"")] =~ \""{1}\""";
$VirtualMachinesWhereClause = "where (type =~ \""microsoft.compute/virtualmachines\""";
$ResourceGroupWhereClause = $VirtualMachinesWhereClause + " and resourceGroup in~ ({0})";
$WhereClause = " | where "
$ArgQueryProjectClause = " | project id, location, name, tags = todynamic(tolower(tostring(tags)))";
$ArgQueryOsTypeClause = " | where properties.storageProfile.osDisk.osType =~ \""{0}\""";
$ArgQueryPatchSettingsClause = "| extend patchSettings = coalesce(properties.osProfile.windowsConfiguration.patchSettings, properties.osProfile.linuxConfiguration.patchSettings)
| extend assessmentMode = tostring(patchSettings.assessmentMode), patchMode = tostring(patchSettings.patchMode), bypassPlatformSafetyChecksOnUserSchedule = tostring(patchSettings.automaticByPlatformSettings.bypassPlatformSafetyChecksOnUserSchedule)"
$ArgDynamicScopeConfigurationAssignmentsClause = "MaintenanceResources
| where type =~ \""microsoft.maintenance/configurationassignments\""
| where properties.maintenanceConfigurationId =~ \""{0}\""
| where properties.resourceId !has \""providers/\""
| extend resourceId = tostring(properties.resourceId)
| project id, type, resourceGroup, subscriptionId, tenantId, location, name, properties";
$AndParam = " and ";
$OrParam = " or ";
$CommaSeparator = ",";
$ForwardSlashSeparator = "/";
$QuoteParam = """";

# Validation values.
$ResourceTypes = @($AzureVM, $ArcServer, $NonAzureMachine)
$OsTypes = @($Windows, $Linux)
$PatchSettingsStatus = @($NotAttempted, $Succeeded, $Failed)
$TelemetryLevels = @($Debug, $Verbose, $Informational, $Warning, $ErrorLvl)
$MachineResourceTypes = @($VMResourceType, $ArcVMResourceType)
$HttpMethods = @($GET, $PATCH, $POST, $PUT)

# Time Zones List for Backward Compatability
$TimeZonesBackwardCompatability = @{
    "America/Nuuk" = "America/Godthab";
    "Asia/Kolkata" = "Asia/Calcutta";
    "Asia/Kathmandu" = "Asia/Katmandu";
    "Asia/Yangon" = "Asia/Rangoon";
    "Asia/Ho_Chi_Minh" = "Asia/Saigon";
    "Atlantic/Faroe" = "Atlantic/Faeroe";
    "Europe/Kyiv" = "Europe/Kiev"
}

# Class for assessment, pathching and error details of machine for onboarding to Azure Update Manager.
class MachineReadinessData
{
    [String]$ResourceId
    [String]$ComputerName
    [String]$ResourceType
    [String]$OsType
    [String]$PeriodicAssessmentStatus
    [String]$ScheduledPatchingStatus
    [String]$ErrorCode
    [String]$ErrorMessage

    [void] UpdateMachineStatus(
        [String]$resourceId,
        [String]$computerName,
        [String]$resourceType,
        [String]$osType,
        [String]$periodicAssessmentStatus = $NotAttempted,
        [String]$scheduledPatchingStatus = $NotAttempted,
        [String]$errorCode,
        [String]$errorMessage)
    {
        <#
            .SYNOPSIS
                Updates status of machines in regards to assessment or scheduled patching status.
        
            .DESCRIPTION
                This function will add/edit machine readiness data.
        
            .PARAMETER resourceId
                ARM resourceId of the machine.      
        
            .PARAMETER computerName
                Computer name.

            .PARAMETER resourceType
                Resource type.
            
            .PARAMETER osType
                OS type.
            
            .PARAMETER preriodicAssessmentStatus
                AssessmentMode status.
    
            .PARAMETER scheduledPatchingStatus
                Scheduled patching status.

            .PARAMETER errorCode
                Error code for setting machine status.
            
            .PARAMETER errorMessage
                Error message for setting machine status.        
        #>

        $this.ResourceId = $resourceId
        $this.ComputerName = $computerName
        $this.ResourceType = $resourceType
        $this.OsType = $osType
        $this.PeriodicAssessmentStatus = $periodicAssessmentStatus
        $this.ScheduledPatchingStatus = $scheduledPatchingStatus
        $this.ErrorCode = $errorCode
        $this.ErrorMessage = $errorMessage
    }
}

# Class for machine association details with respect to software update configuration for onboarding to Azure Update Manager.
class MachineReadinessDataForSoftwareUpdateConfiguration
{
    [String]$ResourceId
    [bool]$IsConfigAssignmentRequired
    [bool]$IsConfigAssignmentSuccessful
    [bool]$IsMachineResolvedThroughAzureDynamicQuery
    [String]$ErrorCode
    [String]$ErrorMessage
}

# Class for dynamic azure queries association details with respect to software update configuration.
class DynamicAzureQueriesDataForSoftwareUpdateConfiguration
{
    [bool]$HasDynamicAzureQueries
    [bool]$AllDynamicAzureQueriesSuccessfullyResolved
    [bool]$AllDynamicAzureQueriesSuccessfullyAssigned
}

# Class for software update configuration migration details.
class SoftwareUpdateConfigurationMigrationData
{
    [String]$SoftwareUpdateConfigurationResourceId    
    [String]$MaintenanceConfigurationResourceId
    [String]$OperatingSystem
    [System.Collections.Hashtable]$MachinesReadinessDataForSoftwareUpdateConfiguration
    [DynamicAzureQueriesDataForSoftwareUpdateConfiguration]$DynamicAzureQueriesStatus
    [bool]$HasSavedSearchQueries
    [bool]$HasPrePostTasks
    [String]$MigrationStatus
    [System.Collections.ArrayList]$ErrorMessage
    [bool]$UnderlyingScheduleDisabled

    SoftwareUpdateConfigurationMigrationData()
    {
        $this.MachinesReadinessDataForSoftwareUpdateConfiguration = @{}
        $this.DynamicAzureQueriesStatus = [DynamicAzureQueriesDataForSoftwareUpdateConfiguration]::new()
        $this.ErrorMessage = [System.Collections.ArrayList]@()
        $this.UnderlyingScheduleDisabled = $false
    }

    [void] UpdateDynamicAzureQueriesStatus(
        [bool]$hasDynamicAzureQueries,
        [bool]$allDynamicAzureQueriesSuccessfullyResolved,
        [bool]$allDynamicAzureQueriesSuccessfullyAssigned)
    {
        <#
            .SYNOPSIS
                Updates status of dynamic azure queries in regards to software update configuration.
        
            .DESCRIPTION
                This function will update dynamic azure queries data in regards to software update configuration.
        
            .PARAMETER hasDynamicAzureQueries
                Whether software update configuration has dynamic azure queries or not.     
        
            .PARAMETER allDynamicAzureQueriesSuccessfullyResolved
                Whether all dynamic azure queries successfully resolved against ARG to get the machines or not.

            .PARAMETER allDynamicAzureQueriesSuccessfullyAssigned
                Whether all dynamic azure queries have successful dynamc scoping assignments or not. 
        #>

        $this.DynamicAzureQueriesStatus.HasDynamicAzureQueries = $hasDynamicAzureQueries
        $this.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyResolved = $allDynamicAzureQueriesSuccessfullyResolved
        $this.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyAssigned = $allDynamicAzureQueriesSuccessfullyAssigned
    }

    [void] UpdateMachineStatusForSoftwareUpdateConfiguration(
        [String]$resourceId,
        [bool]$isConfigAssignmentRequired,
        [bool]$isConfigAssignmentSuccessful,
        [bool]$isMachineResolvedThroughAzureDynamicQuery,
        [String]$errorCode,
        [String]$errorMessage)
    {
        <#
            .SYNOPSIS
                Updates status of machines in regards to software update configuration.
        
            .DESCRIPTION
                This function will add/edit machine readiness data in regards to software update configuration.
        
            .PARAMETER resourceId
                ARM resourceId of the machine.      
        
            .PARAMETER isConfigAssignmentRequired
                Machine attached to schedule or picked up dynamically at run time.

            .PARAMETER isConfigAssignmentSuccessful
                Machine is attached to maintenance configuration or not.
            
            .PARAMETER isMachineResolvedThroughAzureDynamicQuery
                Machine is resolved through dynamic query of SUC or not.

            .PARAMETER errorCode
                Error code for machine association status with maintenance configuration.
            
            .PARAMETER errorMessage
                Error message for machine association status with maintenance configuration.        
        #>
        
        if ($this.MachinesReadinessDataForSoftwareUpdateConfiguration.ContainsKey($resourceId))
        {            
            $this.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId].IsConfigAssignmentRequired = $isConfigAssignmentRequired
            $this.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId].IsConfigAssignmentSuccessful = $isConfigAssignmentSuccessful
            $this.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId].IsMachineResolvedThroughAzureDynamicQuery = $isMachineResolvedThroughAzureDynamicQuery
            $this.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId].ErrorCode = $errorCode
            $this.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId].ErrorMessage = $errorMessage    
        }
        else 
        {
            $machineReadinessDataForSoftwareUpdateConfiguration = [MachineReadinessDataForSoftwareUpdateConfiguration]::new()

            $machineReadinessDataForSoftwareUpdateConfiguration.ResourceId = $resourceId
            $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired = $isConfigAssignmentRequired
            $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentSuccessful = $isConfigAssignmentSuccessful
            $machineReadinessDataForSoftwareUpdateConfiguration.IsMachineResolvedThroughAzureDynamicQuery = $isMachineResolvedThroughAzureDynamicQuery
            $machineReadinessDataForSoftwareUpdateConfiguration.ErrorCode = $errorCode
            $machineReadinessDataForSoftwareUpdateConfiguration.ErrorMessage = $errorMessage

            $this.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId] = $machineReadinessDataForSoftwareUpdateConfiguration
        }
    }
}

#Max depth of payload.
$MaxDepth = 5

# Beginning of Payloads.

$ResourceGroupPayload = @"
{
    "location": null
}
"@

$WindowsAssessmentMode = @"
{
    "properties": {
        "osProfile": {
            "windowsConfiguration": {
                "patchSettings": {
                    "assessmentMode": "AutomaticByPlatform"
                }
            }
        }
    }
}
"@

$LinuxAssessmentMode = @"
{
    "properties": {
        "osProfile": {
            "linuxConfiguration": {
                "patchSettings": {
                    "assessmentMode": "AutomaticByPlatform"
                }
            }
        }
    }
}
"@

$WindowsPatchSettingsOnAzure = @"
{
    "properties": {
        "osProfile": {
            "windowsConfiguration": {
                "patchSettings": {
                    "assessmentMode": "AutomaticByPlatform",
                    "patchMode": "AutomaticByPlatform",
                    "automaticByPlatformSettings": {
                        "bypassPlatformSafetyChecksOnUserSchedule": true
                    }
                }
            }
        }
    }
}
"@

$LinuxPatchSettingsOnAzure = @"
{
    "properties": {
        "osProfile": {
            "linuxConfiguration": {
                "patchSettings": {
                    "assessmentMode": "AutomaticByPlatform",
                    "patchMode": "AutomaticByPlatform",
                    "automaticByPlatformSettings": {
                        "bypassPlatformSafetyChecksOnUserSchedule": true
                    }
                }
            }
        }
    }
}
"@

$MaintenanceConfigurationSettings = @"
{
    "location": "",
    "properties": {
      "namespace": null,
      "extensionProperties": {
        "InGuestPatchMode" : "User"
      },
      "maintenanceScope": "InGuestPatch",
      "maintenanceWindow": {
        "startDateTime": null,
        "expirationDateTime": null,
        "duration": null,
        "timeZone": null,
        "recurEvery": null
      },
      "visibility": "Custom",
      "installPatches": {
        "rebootSetting": null,
        "windowsParameters": {
          "classificationsToInclude": [
          ],
          "kbNumbersToInclude": [            
          ],
          "kbNumbersToExclude": [            
          ]
        },
        "linuxParameters": {
          "classificationsToInclude": [
          ],
          "packageNameMasksToInclude": [            
          ],
          "packageNameMasksToExclude": [            
          ]
        }
      }
    }    
}
"@

$MaintenanceConfigurationAssignmentSettings = @"
{
    "properties": 
    {
        "maintenanceConfigurationId": null
    },
    "location": null
}
"@

$MaintenanceConfigurationDynamicScopingSettings = @"
{
    "location": null,
    "properties": {
      "maintenanceConfigurationId": null,
      "filter": {
        "tagSettings": {
          "tags": {
          },
          "filterOperator": "All"
        },
        "resourceTypes": [
            "Microsoft.Compute/virtualMachines"
        ],
        "locations": [
        ],
        "resourceGroups": [
        ],
        "osTypes": [
        ]
      }
    }
}
"@

$DisableAutomationSchedule = @"
{
    "properties": 
    {
        "isEnabled": false
    }
}
"@
# End of Payloads.

$MachinesOnboaredToAutomationUpdateManagementQuery = 'Heartbeat | where Solutions contains "updates" | distinct Computer, ResourceId, ResourceType, OSType'

$Global:Machines = @{}
$Global:AutomationAccountRegion = $null
$Global:JobSchedules = @{}
$Global:SoftwareUpdateConfigurationsResourceIDs = @{}
$Global:MachinesRetrievedFromLogAnalyticsWorkspaceWithUpdatesSolution = 0
$Global:MachinesRetrievedFromAzureDynamicQueriesWhichAreNotOnboardedToAutomationUpdateManagement = 0

function Write-Telemetry
{
    <#
    .Synopsis
        Writes telemetry to the job logs.
        Telemetry levels can be "Informational", "Warning", "Error" or "Verbose".
    
    .PARAMETER Message
        Log message to be written.
    
    .PARAMETER Level
        Log level.

    .EXAMPLE
        Write-Telemetry -Message Message -Level Level.
    #>
    param(
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$Message,

        [Parameter(Mandatory = $false, Position = 2)]
        [ValidateScript({ $_ -in $TelemetryLevels })]
        [String]$Level = $Informational
    )

    if ($Level -eq $Warning)
    {
        Write-Warning $Message
    }
    elseif ($Level -eq $ErrorLvl)
    {
        Write-Error $Message
    }
    else
    {
        Write-Verbose $Message -Verbose
    }
}

function Parse-ArmId
{
    <#
        .SYNOPSIS
            Parses ARM resource id.
    
        .DESCRIPTION
            This function parses ARM id to return subscription, resource group, resource name, etc.
    
        .PARAMETER ResourceId
            ARM resourceId of the machine.      
    
        .EXAMPLE
            Parse-ArmId -ResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"
    #>
    param(
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$ResourceId
    )

    $parts = $ResourceId.Split("/")
    return @{
        Subscription = $parts[2]
        ResourceGroup = $parts[4]
        ResourceProvider = $parts[6]
        ResourceType = $parts[7]
        ResourceName = $parts[8]
    }
}

function Invoke-RetryWithOutput
{
    <#
        .SYNOPSIS
            Generic retry logic.
    
        .DESCRIPTION
            This command will perform the action specified until the action generates no errors, unless the retry limit has been reached.
    
        .PARAMETER Command
            Accepts an Action object.
            You can create a script block by enclosing your script within curly braces.     
    
        .PARAMETER Retry
            Number of retries to attempt.
    
        .PARAMETER Delay
            The maximum delay (in seconds) between each attempt. The default is 60 second.
    
        .EXAMPLE
            $cmd = { If ((Get-Date) -lt (Get-Date -Second 59)) { Get-Object foo } Else { Write-Host 'ok' } }
            Invoke-RetryWithOutput -Command $cmd -Retry 61
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [ScriptBlock]$Command,
    
        [Parameter(Mandatory = $false, Position = 2)]
        [ValidateRange(0, [UInt32]::MaxValue)]
        [UInt32]$Retry = 3,
    
        [Parameter(Mandatory = $false, Position = 3)]
        [ValidateRange(0, [UInt32]::MaxValue)]
        [UInt32]$Delay = 60
    )
    
    $ErrorActionPreferenceToRestore = $ErrorActionPreference
    $ErrorActionPreference = "Stop"
        
    for ($i = 0; $i -lt $Retry; $i++) 
    {
        $exceptionMessage = ""
        try 
        {            
            Write-Telemetry -Message ("[Debug]Command [{0}] started. Retry: {1}." -f $Command, ($i + 1) + $ForwardSlashSeparator + $Retry)
            $output = Invoke-Command $Command
            Write-Telemetry -Message ("[Debug]Command [{0}] succeeded." -f $Command) 
            $ErrorActionPreference = $ErrorActionPreferenceToRestore
            return $output
        }
        catch [Exception] 
        {
            $exceptionMessage = $_.Exception.Message
                
            if ($Global:Error.Count -gt 0) 
            {
                $Global:Error.RemoveAt(0)
            }

            if ($i -eq ($Retry - 1)) 
            {
                $message = ("[Debug]Command [{0}] failed even after [{1}] retries. Exception message:{2}." -f $command, $Retry, $exceptionMessage)
                Write-Telemetry -Message $message -Level $ErrorLvl
                $ErrorActionPreference = $ErrorActionPreferenceToRestore
                throw $message
            }

            $exponential = [math]::Pow(2, ($i + 1))
            $retryDelaySeconds = ($exponential - 1) * $Delay  # Exponential Backoff Max == (2^n)-1
            Write-Telemetry -Message ("[Debug]Command [{0}] failed. Retrying in {1} seconds, exception message:{2}." -f $command, $retryDelaySeconds, $exceptionMessage) -Level $Warning
            Start-Sleep -Seconds $retryDelaySeconds
        }
    }
}

function Invoke-AzRestApiWithRetry
{
   <#
        .SYNOPSIS
            Wrapper around Invoke-AzRestMethod.
    
        .DESCRIPTION
            This function calls Invoke-AzRestMethod with retries.
    
        .PARAMETER Params
            Parameters to the cmdlet.

        .PARAMETER Payload
            Payload.

        .PARAMETER Retry
            Number of retries to attempt.
    
        .PARAMETER Delay
            The maximum delay (in seconds) between each attempt. The default is 60 second.
            
        .EXAMPLE
            Invoke-AzRestApiWithRetry -Params @{SubscriptionId = "xxxx" ResourceGroup = "rgName" ResourceName = "resourceName" ResourceProvider = "Microsoft.Compute" ResourceType = "virtualMachines"} -Payload "{'location': 'westeurope'}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [System.Collections.Hashtable]$Params,

        [Parameter(Mandatory = $false, Position = 2)]
        [Object]$Payload = $null,

        [Parameter(Mandatory = $false, Position = 3)]
        [ValidateRange(0, [UInt32]::MaxValue)]
        [UInt32]$Retry = 3,
    
        [Parameter(Mandatory = $false, Position = 4)]
        [ValidateRange(0, [UInt32]::MaxValue)]
        [UInt32]$Delay = 60
    )

    if ($Payload)
    {
        [void]$Params.Add('Payload', $Payload)
    }

    $retriableErrorCodes = @(409, 429)
        
    for ($i = 0; $i -lt $Retry; $i++)
    {
        $exceptionMessage = ""
        $paramsString = $Params | ConvertTo-Json -Compress -Depth $MaxDepth | ConvertFrom-Json
        try
        {
            Write-Telemetry -Message ("[Debug]Invoke-AzRestMethod started with params [{0}]. Retry: {1}." -f $paramsString, ($i+1) + $ForwardSlashSeparator + $Retry)
            $output = Invoke-AzRestMethod @Params -ErrorAction Stop
            $outputString = $output | ConvertTo-Json -Compress -Depth $MaxDepth | ConvertFrom-Json
            if ($retriableErrorCodes.Contains($output.StatusCode) -or $output.StatusCode -ge 500)
            {
                if ($i -eq ($Retry - 1))
                {
                    $message = ("[Debug]Invoke-AzRestMethod with params [{0}] failed even after [{1}] retries. Failure reason:{2}." -f $paramsString, $Retry, $outputString)
                    Write-Telemetry -Message $message -Level $ErrorLvl
                    return Process-ApiResponse -Response $output
                }

                $exponential = [math]::Pow(2, ($i+1))
                $retryDelaySeconds = ($exponential - 1) * $Delay  # Exponential Backoff Max == (2^n)-1
                Write-Telemetry -Message ("[Debug]Invoke-AzRestMethod with params [{0}] failed with retriable error code. Retrying in {1} seconds, Failure reason:{2}." -f $paramsString, $retryDelaySeconds, $outputString) -Level $Warning
                Start-Sleep -Seconds $retryDelaySeconds
            }
            else
            {
                Write-Telemetry -Message ("[Debug]Invoke-AzRestMethod with params [{0}] succeeded. Output: [{1}]." -f $paramsString, $outputString)
                return Process-ApiResponse -Response $output
            }
        }
        catch [Exception]
        {
            $exceptionMessage = $_.Exception.Message
            Write-Telemetry -Message ("[Debug]Invoke-AzRestMethod with params [{0}] failed with an unhandled exception: {1}." -f $paramsString, $exceptionMessage) -Level $ErrorLvl
            throw
        }
    }   
}

function Invoke-ArmApi-WithPath
{
   <#
        .SYNOPSIS
            The function prepares payload for Invoke-AzRestMethod
    
        .DESCRIPTION
            This function prepares payload for Invoke-AzRestMethod.
    
        .PARAMETER Path
            ARM API path.

        .PARAMETER ApiVersion
            API version.

        .PARAMETER Method
            HTTP method.

        .PARAMETER Payload
            Paylod for API call.
    
        .EXAMPLE
            Invoke-ArmApi-WithPath -Path "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Compute/virtualMachines/{vmName}/start" -ApiVersion "2023-03-01" -method "PATCH" -Payload "{'location': 'westeurope'}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$Path,

        [Parameter(Mandatory = $true, Position = 2)]
        [String]$ApiVersion,

        [Parameter(Mandatory = $true, Position = 3)]
        [ValidateScript({ $_ -in $HttpMethods })]
        [String]$Method,

        [Parameter(Mandatory = $false, Position =4)]
        [Object]$Payload = $null
    )

    $PathWithVersion = "{0}?api-version={1}"
    if ($Path.Contains("?"))
    {
        $PathWithVersion = "{0}&api-version={1}"
    }

    $Uri = ($PathWithVersion -f $Path, $ApiVersion) 
    $Params = @{
        Path = $Uri
        Method = $Method
    }

    return Invoke-AzRestApiWithRetry -Params $Params -Payload $Payload   
}

function Process-ApiResponse
{
    <#
        .SYNOPSIS
            Process API response and returns data.
    
        .PARAMETER Response
            Response object.
    
        .EXAMPLE
            Process-ApiResponse -Response {"StatusCode": 200, "Content": "{\"properties\": {\"location\": \"westeurope\"}}" }
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [Object]$Response
    )

    $content = $null
    if ($Response.Content)
    {
        $content = ConvertFrom-Json $Response.Content
    }

    if ($Response.StatusCode -eq 200)
    {
        return @{ 
            Status = $Succeeded
            Response = $content
            ErrorCode = [String]::Empty 
            ErrorMessage = [String]::Empty
            }
    }
    else
    {
        $errorCode = $Unknown
        $errorMessage = $Unknown
        if ($content.error)
        {
            $errorCode = ("{0}/{1}" -f $Response.StatusCode, $content.error.code)
            $errorMessage = $content.error.message
        }

        return @{ 
            Status = $Failed
            Response = $content
            ErrorCode = $errorCode  
            ErrorMessage = $errorMessage
            }
    }
}

function Enable-PeriodicAssessment
{
   <#
        .SYNOPSIS
            Enables periodic assessment.
    
        .DESCRIPTION
            This command will set assessmentMode to "AutomaticByPlatform".

        .PARAMETER ResourceId
            Resource Id.

        .PARAMETER ResourceType
            Resource type.
    
        .PARAMETER OsType
            Operating system of VM.
    
        .EXAMPLE
            Enable-PeriodicAssessment -ResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Compute/virtualMachines/{vmName}" -ResourceType "AzureVM" -OsType "Windows"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$ResourceId,

        [Parameter(Mandatory = $true, Position = 2)]
        [ValidateScript({ $_ -in $ResourceTypes })]
        [String]$ResourceType,

        [Parameter(Mandatory = $true, Position = 3)]
        [ValidateScript({ $_ -in $OsTypes })]
        [String]$OsType
    )
    
    $payload = if ($OsType -eq $Windows) { $WindowsAssessmentMode } else { $LinuxAssessmentMode }
    $version = if ($ResourceType -eq $ArcServer) { $ArcVmApiVersion } else { $VmApiVersion }

    try
    {
        $output = Invoke-ArmApi-WithPath -Path $ResourceId -ApiVersion $version -Method $PATCH -Payload $payload
        return $output
    }
    catch [Exception]
    {
        return @{ 
            Status = $Failed 
            ErrorCode = $UnhandledException  
            ErrorMessage = $_.Exception.Message
            }
    }
    
    return $output    
}

function Escape-SpecialCharacters
{
    <#
        .SYNOPSIS
            Escapes special characters.
    
        .PARAMETER Value
            String to escape.

        .EXAMPLE
            Escape-SpecialCharacters -Value Value
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [String]$Value
    )

    return $Value.Replace("\", "\\").Replace("""", "\""");
}

function Build-ResourceGraphQuery
{
    <#
        .SYNOPSIS
            Build the resource graph query.
    
        .DESCRIPTION
            This command will build and return the resource graph query in desired format.
    
        .PARAMETER AzureQuery
            Azure query.

        .PARAMETER OsType
            OS type.

        .PARAMETER ResourceGroups
            Resource groups.

        .EXAMPLE
            Build-ResourceGraphQuery -AzureQuery {"scope" : "" , "tagSettings" : {"tag1": ["tag1Value1","tag1Value2"],"tag2": ["tag2Value1","tag2Value2"]}, "filterOperator" : "", "locations" : ""} -OsType "Windows" -ResourceGroups "\"rg1\"", \"rg2\"" 
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [Object]$AzureQuery,

        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        [String]$OsType,

        [Parameter(Mandatory = $false, Position = 3, ValueFromPipeline = $true)]
        [String]$ResourceGroups = $null
    )

    $query = ""

    if ($ResourceGroups)
    {
        $query += ($ResourceGroupWhereClause -f $ResourceGroups)
    }
    else 
    {
        $query += $VirtualMachinesWhereClause;
    }

    if (($null -ne $AzureQuery.locations) -and ($AzureQuery.locations.Length -gt 0))
    {
        $locations = ($AzureQuery.locations) | %{$QuoteParam + $_.Replace(" ", "") + $QuoteParam}
        $locationsString = Escape-SpecialCharacters -Value ($locations -join $CommaSeparator)

        $query +=" and location in~(" + $locationsString + ")"
    }

    $query += ") " + ($ArgQueryOsTypeClause -f $OsType)

    if ($null -ne $AzureQuery.tagSettings.tags -and $AzureQuery.tagSettings.tags.psobject.properties.Value.Count -gt 0)
    {
        $lowerCaseTagsQuery = $ArgQueryProjectClause + $WhereClause
        $tagsKqlExpression = ""
        $op = $AndParam
        if ($AzureQuery.tagSettings.filterOperator -eq "Any")
        {
            $op = $OrParam
        } 

        $tagFilters = [System.Collections.ArrayList]@()
        $tags = $AzureQuery.tagSettings.tags
        foreach ($property in $tags.psobject.properties)
        {
            $escapedKey = Escape-SpecialCharacters -Value $property.Name
            foreach ($val in $property.Value)
            {
                $escapedValue = Escape-SpecialCharacters -Value $val
                $str = ($TagFilterClause -f $escapedKey, $escapedValue)
                $tagFilters += $str
            }
        }

        $tagQuery = ($tagFilters -Join $op)
        $query = $query + $lowerCaseTagsQuery + " (" + $tagQuery + ")"
    }

    $query = $QuoteParam + $query + " | project id" + $QuoteParam
    return $query
}

function Get-ResourceGraphApiPayload
{
    <#
        .SYNOPSIS
            Build the resource graph query API payload.
    
        .DESCRIPTION
            This command will build the ARG query payload.
    
        .PARAMETER AzureQuery
            Azure query.

        .PARAMETER OsType
            OS type.

        .PARAMETER ResourceGroups
            Resource groups.

        .EXAMPLE
            Get-ResourceGraphApiPayload -AzureQuery {"scope" : "" , "tagSettings" : {"tag1": ["tag1Value1","tag1Value2"],"tag2": ["tag2Value1","tag2Value2"]}, "filterOperator" : "", "locations" : ""} -OsType "Windows" -SubscriptionId "xxxx-xxx" -ResourceGroups "\"rg1\"", \"rg2\"" 
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [Object]$AzureQuery,

        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        [String]$OsType,

        [Parameter(Mandatory = $true, Position = 3, ValueFromPipeline = $true)]
        [String]$SubscriptionId,

        [Parameter(Mandatory = $false, Position = 4, ValueFromPipeline = $true)]
        $ResourceGroups = $null
    )

    $subscriptionIdFormatted = $QuoteParam + $SubscriptionId + $QuoteParam
    $rgsFormatted = $null

    if ($ResourceGroups)
    {
        $rgs = ($ResourceGroups) | %{$QuoteParam + $_ + $QuoteParam}
        $rgsFormatted = Escape-SpecialCharacters -Value ($rgs -Join $CommaSeparator)
    }
    
    $query = Build-ResourceGraphQuery -AzureQuery $AzureQuery -OsType $OsType -ResourceGroups $rgsFormatted
    $argPayloadObject = @"
    { 
        "subscriptions": [$subscriptionIdFormatted], 
        "query": $query
    }
"@
    $argPayload = $argPayloadObject | ConvertTo-Json -Compress -Depth $MaxDepth | ConvertFrom-Json
    return $argPayload
}

function Process-DynamicAzureQueryToArgPayload
{
   <#
        .SYNOPSIS
            Processes dynamic azure queries and converts to ARG payload.
    
        .DESCRIPTION
            This command will process each dynamic query, construct corresponding ARG query payloads to make ARG calls.
    
        .PARAMETER AzureQuery
            Azure query.
        
        .PARAMETER OsType
            Operating system type.

        .EXAMPLE
            Process-DynamicAzureQueryToArgPayload -AzureQuery {"scope" : "" , "tagSettings" : {"tag1": ["tag1Value1","tag1Value2"],"tag2": ["tag2Value1","tag2Value2"]}, "filterOperator" : "", "locations" : ""} -OsType "Windows"      
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        $AzureQueries,

        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        [String]$OsType
    )

    $payloadList = [System.Collections.Generic.HashSet[String]]@()

    foreach ($azureQuery in $AzureQueries)
    {
        $subscriptionsOnly = [System.Collections.Generic.HashSet[String]]@()
        $resourceGroups = @{}

        # Get the Scope Details of the dynamic query
        foreach ($scope in $azureQuery.scope)
        {
            $scopeLowerCase = $scope.ToLower()
            if ($scopeLowerCase -Match $ScopeRgRegExPattern)
            {
                $parts = $scopeLowerCase.Split("/")
                if (!$resourceGroups.ContainsKey($parts[2]))
                {
                    $resourceGroups[$parts[2]] = [System.Collections.Generic.HashSet[String]]@()
                }

                [void]$resourceGroups[$parts[2]].Add($parts[4])
            }
            elseif ($scopeLowerCase -Match $ScopeSubscriptionRegExPattern)
            {
                $parts = $scopeLowerCase.Split("/")
                [void]$subscriptionsOnly.Add($parts[2])
            }
            else
            {
                Write-Telemetry -Message ("Invalid dynamic group scope {0}." -f $scopeLowerCase) -Level $Warning
            }   
        }

        if (($resourceGroups.Length -eq 0) -and ($subscriptionsOnly.Length -eq 0))
        {
            Write-Telemetry -Message ("Invalid dynamic group query {0}." -f $scopeLowerCase) -Level $Warning
            continue
        }

        foreach ($iter in $resourceGroups.Keys)
        {
            $argPayload = Get-ResourceGraphApiPayload -AzureQuery $azureQuery -OsType $OsType -SubscriptionId $iter -ResourceGroups $resourceGroups[$iter]
            [void]$payloadList.Add($argPayload)
        }

        foreach ($sub in $subscriptionsOnly)
        {
            $argPayload = Get-ResourceGraphApiPayload -AzureQuery $azureQuery -OsType $OsType -SubscriptionId $sub
            [void]$payloadList.Add($argPayload)
        }
    }

    return $payloadList
}

function Get-MachinesFromAzureDynamicQueries
{
   <#
        .SYNOPSIS
            Get all azure machines from azure dynamic queries for the given software update configuration.
    
        .DESCRIPTION
            This command will
            1. Get all azure machines from azure dynamic queries for the given software update configuration.
    
        .PARAMETER SoftwareUpdateConfigurationMigrationData
            Software update configuration migration data object.

        .PARAMETER AzureQueries
            List of azure queries.
        
        .EXAMPLE
            Get-MachinesFromAzureDynamicQueries -SoftwareUpdateConfigurationMigrationData SoftwareUpdateConfigurationMigrationData -AzureQueries AzureQueries
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [SoftwareUpdateConfigurationMigrationData]$SoftwareUpdateConfigurationMigrationData,

        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        [System.Collections.ArrayList]$AzureQueries
    )

    $resourceIds = [System.Collections.Generic.HashSet[String]]@()
    $payloadList = Process-DynamicAzureQueryToArgPayload -AzureQueries $AzureQueries -OsType $SoftwareUpdateConfigurationMigrationData.OperatingSystem

    $countOfFailedResolutions = 0

    foreach ($payload in $payloadList)
    {
        try 
        {
            $argPath = ("/providers/{0}/{1}" -f $ResourceGraphRP, $ArgResourcesResourceType)
            $records = Invoke-ArmApi-WithPath -Path $argPath -ApiVersion $ResourceGraphApiVersion -Method $POST -Payload $payload
            if ($records.Status -eq $Failed)
            {
                Write-Telemetry -Message ("Failed to get machines for the dynamic query {0} with error code {1} and error message {2}." -f $payload, $records.ErrorCode, $records.ErrorMessage) -Level $ErrorLvl
                $countOfFailedResolutions++
            }
            foreach ($record in $records.Response.data)
            {
                [void]$resourceIds.Add($record.id)
                if (!$Global:Machines.Contains($record.id))
                {
                    $machineReadinessData = [MachineReadinessData]::new()
                    $machineReadinessData.UpdateMachineStatus($record.id, $null, $AzureVM, $SoftwareUpdateConfigurationMigrationData.OperatingSystem, $NotAttempted, $NotAttempted, [String]::Empty, [String]::Empty)
                    $Global:Machines[$machineReadinessData.ResourceId] = $machineReadinessData
                    $Global:MachinesRetrievedFromAzureDynamicQueriesWhichAreNotOnboardedToAutomationUpdateManagement++
                }
            }
        }
        catch [Exception]
        {
            $countOfFailedResolutions++
            $exceptionMessage = $_.Exception.Message
            Write-Telemetry -Message ("Unhandled exception in Get-MachinesFromAzureDynamicQueries: {0}" -f $exceptionMessage) -Level $ErrorLvl
        }
    }

    if ($countOfFailedResolutions -eq 0)
    {
        $SoftwareUpdateConfigurationMigrationData.UpdateDynamicAzureQueriesStatus($SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.HasDynamicAzureQueries, $true, $SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyAssigned)
    }

    return $resourceIds
}

function Enable-ScheduledPatching
{
   <#
        .SYNOPSIS
            Sets scheduled patching properties on Azure VM.
    
        .DESCRIPTION
            This command will set patchMode to "AutomaticByPlatform" and bypass flag to true for Azure VM.
        
        .PARAMETER ResourceId
            Resource Id.
    
        .PARAMETER OsType
            Operating system of VM.
    
        .EXAMPLE
            Enable-ScheduledPatching -resourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Compute/virtualMachines/{vmName}" -osType "Windows"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$ResourceId,

        [Parameter(Mandatory = $true, Position = 2)]
        [ValidateScript({ $_ -in $OsTypes })]
        [String]$OsType
    )
    
    $payload = if ($OsType -eq $Windows) { $WindowsPatchSettingsOnAzure } else { $LinuxPatchSettingsOnAzure }
    $version = $VmApiVersion

    try
    {
        $output = Invoke-ArmApi-WithPath -Path $ResourceId -ApiVersion $version -Method $PATCH -Payload $payload
        return $output
    }
    catch [Exception]
    {
        return @{ 
            Status = $Failed 
            ErrorCode = $UnhandledException  
            ErrorMessage = $_.Exception.Message
            }
    }
    
    return $output
}

function Get-MachinesFromLogAnalytics
{
   <#
        .SYNOPSIS
            Gets machines onboarded to updates solution from Log Analytics workspace.
    
        .DESCRIPTION
            This command will return machines onboarded to UM from LA workspace.

        .PARAMETER ResourceId
            Resource Id.

        .EXAMPLE
            Get-MachinesFromLogAnalytics -ResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$ResourceId
    )
    
    $armComponents = Parse-ArmId -ResourceId $ResourceId
    $script = {
        Set-AzContext -Subscription $armComponents.Subscription
        $Workspace = Get-AzOperationalInsightsWorkspace -ResourceGroupName $armComponents.ResourceGroup -Name $armComponents.ResourceName
        $QueryResults = Invoke-AzOperationalInsightsQuery -WorkspaceId $Workspace.CustomerId -Query $MachinesOnboaredToAutomationUpdateManagementQuery -ErrorAction Stop
        return $QueryResults
    }

    $output = Invoke-RetryWithOutput -command $script
    return $output  
}

function Enable-PeriodicAssessmentForMachines
{
    <#
        .SYNOPSIS
            Enables periodic assessment for machines passed as input.
    
        .DESCRIPTION
            This function enables periodic assessment.
    
        .PARAMETER Machines
            Machines.
    
        .EXAMPLE
            Enable-PeriodicAssessmentForMachines -Machines Machines
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [AllowEmptyCollection()]
        [System.Collections.ArrayList]$Machines
    )

    try
    {
        $resourceIdList = $Machines

        foreach ($resourceId in $resourceIdList)
        {
            $machine = $Global:Machines[$resourceId]
            if ($machine.ResourceType -ne $NonAzureMachine -and $machine.PeriodicAssessmentStatus -ne $Succeeded)
            {
                $periodicAssessmentStatus = Enable-PeriodicAssessment -ResourceId $machine.ResourceId -ResourceType $machine.ResourceType -OsType $machine.OsType
                $machine.UpdateMachineStatus(
                    $machine.ResourceId,
                    $machine.ComputerName,
                    $machine.ResourceType,
                    $machine.OsType,
                    $periodicAssessmentStatus.Status,
                    $machine.ScheduledPatchingStatus,
                    $periodicAssessmentStatus.ErrorCode,
                    $periodicAssessmentStatus.ErrorMessage)
                
                if ($periodicAssessmentStatus.Status -eq $Succeeded)
                {
                    Write-Telemetry -Message ("Enabled periodic assessment for machine `n {0}" -f ($machine | ConvertTo-Json -Depth $MaxDepth))     
                }
                else
                {
                    Write-Telemetry -Message ("Failed to enable periodic assessment for machine `n {0}" -f ($machine | ConvertTo-Json -Depth $MaxDepth)) -Level $ErrorLvl
                }
            }
        }    
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Unhandled Exception while enabling periodic assessment {0}." -f $_.Exception.Message) -Level $ErrorLvl
    }
}

function Enable-PatchSettingsForMachines
{
    <#
        .SYNOPSIS
            Enables patch settings for azure/arc machines passed.
    
        .DESCRIPTION
            This function enables patch settings required for scheduled patching and periodic assessment.
    
        .PARAMETER Machines
            Machines.
    
        .EXAMPLE
            Enable-PatchSettingsForMachines -Machines Machines
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [AllowEmptyCollection()]
        [System.Collections.ArrayList]$Machines
    )

    try
    {
        $resourceIdList = $Machines

        Enable-PeriodicAssessmentForMachines -Machines $resourceIdList
    
        foreach ($resourceId in $resourceIdList)
        {
            $machine = $Global:Machines[$resourceId]
            if ($machine.ResourceType -eq $AzureVM -and $machine.ScheduledPatchingStatus -ne $Succeeded)
            {
                $scheduledPatchingStatus = Enable-ScheduledPatching -ResourceId $machine.ResourceId -OsType $machine.OsType
                $machine.UpdateMachineStatus(
                    $machine.ResourceId,
                    $machine.ComputerName,
                    $machine.ResourceType,
                    $machine.OsType,
                    $machine.PeriodicAssessmentStatus,
                    $scheduledPatchingStatus.Status,
                    $scheduledPatchingStatus.ErrorCode,
                    $scheduledPatchingStatus.ErrorMessage)
    
                if ($scheduledPatchingStatus.Status -eq $Succeeded)
                {
                    Write-Telemetry -Message ("Enabled patch settings for machine `n {0}" -f ($machine | ConvertTo-Json -Depth $MaxDepth))
                }
                else
                {
                    Write-Telemetry -Message ("Failed to enable patch settings for machine `n {0}" -f ($machine | ConvertTo-Json -Depth $MaxDepth)) -Level $ErrorLvl
                }
            }
        }    
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Unhandled Exception while enabling patch settings {0}." -f $_.Exception.Message) -Level $ErrorLvl
    }
}

function Enable-PeriodicAssessmentOnAllMachines
{
    <#
        .SYNOPSIS
            Enables periodic assessment for all machines.
    
        .DESCRIPTION
            This function enables periodic assessment for all machines onboarded to Automation Update Management under this automation account.
    
        .PARAMETER AutomationAccountResourceId
            Automation account resource id.
    
        .EXAMPLE
            Enable-PeriodicAssessmentOnAllMachines -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId
    )

    $machines = [System.Collections.ArrayList]@($Global:Machines.Keys)
    Enable-PeriodicAssessmentForMachines -Machines $machines
        
    $countOfAllMachinesOnboardedToAutomationUpdateManagement = $Global:MachinesRetrievedFromLogAnalyticsWorkspaceWithUpdatesSolution
    $countOfAzureMachinesWithPeriodicAssessmentEnabled = 0
    $countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled = 0
    $countOfAzureMachinesWhereFailedToEnablePeriodicAssessment = 0
    $countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment = 0
    $countOfNonAzureMachinesIgnored = 0
    $countOfNotFoundMachines = 0

    foreach ($machine in $machines)
    {
        if ($Global:Machines[$machine].ResourceType -ne $NonAzureMachine -and $Global:Machines[$machine].ErrorCode -match $NotFoundErrorCode)
        {
            $countOfNotFoundMachines++
        }
        elseif ($Global:Machines[$machine].ResourceType -eq $AzureVM)
        {
            if ($Global:Machines[$machine].PeriodicAssessmentStatus -eq $Succeeded)
            {
                Write-Output ("Enabled periodic assessment for azure machine {0}." -f $machine)
                $countOfAzureMachinesWithPeriodicAssessmentEnabled++
            }
            else
            {
                Write-Output ("Failed to enable periodic assessment for azure machine {0} with error code {1} and error message {2}." -f $machine, $Global:Machines[$machine].ErrorCode, $Global:Machines[$machine].ErrorMessage)
                $countOfAzureMachinesWhereFailedToEnablePeriodicAssessment++
            }
        }
        elseif ($Global:Machines[$machine].ResourceType -eq $ArcServer)
        {
            if ($Global:Machines[$machine].PeriodicAssessmentStatus -eq $Succeeded)
            {
                Write-Output ("Enabled periodic assessment for non-azure arc machine {0} with resource ID {1}." -f $machine , $Global:Machines[$machine].ResourceId)
                $countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled++
            }
            else
            {
                Write-Output ("Failed to enable periodic assessment for non-azure arc machine {0} with resource ID {1} with error code {2} and error message {3}." -f $machine, $Global:Machines[$machine].ResourceId, $Global:Machines[$machine].ErrorCode, $Global:Machines[$machine].ErrorMessage)
                $countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment++
            }
        }
        else
        {
            Write-Output ("Ignored {0} as it is not onboarded to arc." -f $machine)
            $countOfNonAzureMachinesIgnored++
        }
    }

    Write-Output ("Total {0} machines found to be onboarded to Automation Update Management under automation account {1}." -f ($countOfAllMachinesOnboardedToAutomationUpdateManagement - $countOfNotFoundMachines), $AutomationAccountResourceId)
    if ($countOfNonAzureMachinesIgnored -gt 0)
    {
        Write-Output ("{0} non-azure machines which are not onboarded to arc and ignored." -f $countOfNonAzureMachinesIgnored)
    }
    if ($countOfAzureMachinesWithPeriodicAssessmentEnabled -gt 0)
    {
        Write-Output ("{0} azure machines enabled for periodic assessment." -f $countOfAzureMachinesWithPeriodicAssessmentEnabled)
    }
    if ($countOfAzureMachinesWhereFailedToEnablePeriodicAssessment -gt 0)
    {
        Write-Output ("Failed to enable periodic assessment for {0} azure machines." -f $countOfAzureMachinesWhereFailedToEnablePeriodicAssessment)
    }    
    if ($countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled -gt 0)
    {
        Write-Output ("{0} non-azure arc machines enabled for periodic assessment." -f $countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled)
    }
    if ($countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment -gt 0)
    {
        Write-Output ("Failed to enable periodic assessment for {0} non-azure arc machines." -f $countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment)
    }
}

function Populate-AllMachinesOnboardedToUpdateManagement
{
    <#
        .SYNOPSIS
            Gets all machines onboarded to Update Management under this automation account.
    
        .DESCRIPTION
            This function gets all machines onboarded to Automation Update Management under this automation account using log analytics workspace.
    
        .PARAMETER AutomationAccountResourceId
            Automation account resource id.
    
        .EXAMPLE
            Populate-AllMachinesOnboardedToUpdateManagement -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId
    )

    try 
    {
        $linkedWorkspace = Invoke-ArmApi-WithPath -Path ($LinkedWorkspacePath -f $AutomationAccountResourceId) -ApiVersion $AutomationApiVersion -Method $GET
        $laResults = Get-MachinesFromLogAnalytics -ResourceId $linkedWorkspace.Response.Id
        if ($laResults.Results.Count -eq 0 -and $null -eq $laResults.Error)
        {
            Write-Telemetry -Message ("Zero machines retrieved from log analytics workspace. If machines were recently onboarded, please wait for few minutes for machines to start reporting to log analytics workspace") -Level $ErrorLvl
            throw
        }
        elseif ($laResults.Results.Count -gt 0)
        {
            Write-Telemetry -Message ("Retrieved machines from log analytics workspace.")

            foreach ($record in $laResults.Results)
            {
                $machineReadinessData = [MachineReadinessData]::new()
    
                if ($record.ResourceType -eq $ArcVMResourceType)
                {
                    $machineReadinessData.UpdateMachineStatus($record.ResourceId, $record.Computer, $ArcServer, $record.OsType, $NotAttempted, $NotAttempted, [String]::Empty, [String]::Empty)
                    $Global:Machines[$machineReadinessData.ComputerName] = $machineReadinessData
                }
                elseif ($record.ResourceType -eq $VMResourceType)
                {
                    $machineReadinessData.UpdateMachineStatus($record.ResourceId, $null, $AzureVM, $record.OsType, $NotAttempted, $NotAttempted, [String]::Empty, [String]::Empty)
                    $Global:Machines[$machineReadinessData.ResourceId] = $machineReadinessData
                }
                else
                {
                    $machineReadinessData.UpdateMachineStatus($null, $record.Computer, $NonAzureMachine, $record.OsType, $NotAttempted, $NotAttempted, $NotOnboardedToArcErrorCode, $NotOnboardedToArcErrorMessage)
                    $Global:Machines[$machineReadinessData.ComputerName] = $machineReadinessData
                }
                
                $Global:MachinesRetrievedFromLogAnalyticsWorkspaceWithUpdatesSolution++
            }        
        }
        else
        {
            Write-Telemetry -Message ("Failed to get machines from log analytics workspace with error {0}." -f $laResults.Error) -Level $ErrorLvl
            throw
        }
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Unhandled exception {0}." -f, $_.Exception.Message) -Level $ErrorLvl
        throw
    }
}

function Set-MaintenanceConfigurationAssignmentForMachines
{
    <#
        .SYNOPSIS
            Sets configuration assignments for the machines.
    
        .DESCRIPTION
            This function sets configuration assignments for the machines in software update configuration to attach them to the maintenance configuration.
    
        .PARAMETER SoftwareUpdateConfigurationMigrationData
            Software update configration migration data
            
        .EXAMPLE
            Set-MaintenanceConfigurationAssignmentForMachines -SoftwareUpdateConfigurationMigrationData SoftwareUpdateConfigurationMigrationData
    #>
    [CmdletBinding()]
    param 
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [SoftwareUpdateConfigurationMigrationData]$SoftwareUpdateConfigurationMigrationData
    )
    
    $resourceIdList = $SoftwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration.Keys

    foreach ($resourceId in $resourceIdList)
    {
        try
        {
            $machineReadinessDataForSoftwareUpdateConfiguration = $SoftwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId]
            if ($machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired)
            {
                if ($Global:Machines[$resourceId].ResourceType -ne $NonAzureMachine -and $Global:Machines[$resourceId].ErrorCode -notmatch $NotFoundErrorCode)
                {
                    $version = if ($Global:Machines[$resourceId].ResourceType -eq $ArcServer) { $ArcVmApiVersion } else { $VmApiVersion }
                    $response = Invoke-ArmApi-WithPath -Path $Global:Machines[$resourceId].ResourceId -ApiVersion $version -Method $GET

                    $configurationAssignmentPayload = ConvertFrom-Json $MaintenanceConfigurationAssignmentSettings
                    $configurationAssignmentPayload.properties.maintenanceConfigurationId = $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId
                    $configurationAssignmentPayload.location = $response.Response.location
                
                    $configurationAssignmentPayload = ConvertTo-Json $configurationAssignmentPayload -Depth $MaxDepth
                    $parts = $SoftwareUpdateConfigurationMigrationData.SoftwareUpdateConfigurationResourceId.Split("/")
                
                    $response = Invoke-ArmApi-WithPath -Path ($MaintenanceConfigurationAssignmentPath -f $Global:Machines[$resourceId].ResourceId, $MigrationTag + "_" + $parts[8] + "_" + $parts[10] ) -ApiVersion $MaintenanceConfigurationpApiVersion -Method $PUT -Payload $configurationAssignmentPayload
                    if ($response.Status -eq $Failed)
                    {
                        Write-Telemetry -Message ("Configuration assignment failed for resource {0} for the maintenance configuration {1}." -f $resourceId, $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId) -Level $Warning
                        $SoftwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration(
                            $machineReadinessDataForSoftwareUpdateConfiguration.ResourceId,
                            $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired,
                            $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentSuccessful,
                            $machineReadinessDataForSoftwareUpdateConfiguration.IsMachineResolvedThroughAzureDynamicQuery,
                            $response.ErrorCode,
                            $response.ErrorMessage)
                    }
                    else
                    {
                        Write-Telemetry -Message ("Configuration assignment succeeded for resource {0} for the maintenance configuration {1}." -f $resourceId, $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId)
                        $SoftwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration(
                            $machineReadinessDataForSoftwareUpdateConfiguration.ResourceId,
                            $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired,
                            $true,
                            $machineReadinessDataForSoftwareUpdateConfiguration.IsMachineResolvedThroughAzureDynamicQuery,
                            $Global:Machines[$resourceId].ErrorCode,
                            $Global:Machines[$resourceId].ErrorMessage)
                    }
                }
            }    
        }
        catch
        {
            $exceptionMessage = $_.Exception.Message
            Write-Telemetry -Message ("Resource [{0}] failed with an unhandled exception: {1} while setting configuration assignment." -f $resourceId, $exceptionMessage) -Level $ErrorLvl
            $SoftwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration(
                $machineReadinessDataForSoftwareUpdateConfiguration.ResourceId,
                $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired,
                $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentSuccessful,
                $machineReadinessDataForSoftwareUpdateConfiguration.IsMachineResolvedThroughAzureDynamicQuery,
                $Global:Machines[$resourceId].ErrorCode,
                $exceptionMessage)
        }
    }
}

function Validate-DynamicScopeConfigurationAlreadyAssigned
{
    <#
        .SYNOPSIS
            Checks if a dynamic scope configuration assignment already made for the maintenance config or not.
    
        .DESCRIPTION
            This function checks if a dynamic scope configuration assignment already made for the maintenance config or not.
    
        .PARAMETER DynamicScopeConfigurationAssignments
            Dynamic scope configuration assignments

        .PARAMETER MaintenanceConfigurationDynamicScopingPayload
            Maintenance configuration dynamic scoping payload
            
        .EXAMPLE
            Validate-DynamicScopeConfigurationAlreadyAssigned -DynamicScopeConfigurationAssignments DynamicScopeConfigurationAssignments -MaintenanceConfigurationDynamicScopingPayload MaintenanceConfigurationDynamicScopingPayload -Scope $Scope
    #>
    [CmdletBinding()]
    param 
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        $DynamicScopeConfigurationAssignments,

        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        $MaintenanceConfigurationDynamicScopingPayload,

        [Parameter(Mandatory = $true, Position = 3, ValueFromPipeline = $true)]
        [String]$Scope
    )

    foreach ($dynamicScopeConfigurationAssignment in $DynamicScopeConfigurationAssignments)
    {
        $isequal = $true

        if ($dynamicScopeConfigurationAssignment.properties.maintenanceConfigurationId -ne $MaintenanceConfigurationDynamicScopingPayload.properties.maintenanceConfigurationId)
        {
            $isequal = $false
            continue
        }

        if ($dynamicScopeConfigurationAssignment.properties.resourceId -ne $Scope)
        {
            $isequal = $false
            continue
        }

        if ($dynamicScopeConfigurationAssignment.properties.filter.resourceGroups.Length -ne $MaintenanceConfigurationDynamicScopingPayload.properties.filter.resourceGroups.Length)
        {
            $isequal = $false
            continue
        }

        foreach ($resourceGroup in $MaintenanceConfigurationDynamicScopingPayload.properties.filter.resourceGroups)
        {
            if (!($dynamicScopeConfigurationAssignment.properties.filter.resourceGroups -contains $resourceGroup))
            {
                $isequal = $false
                break
            }
        }

        if ($dynamicScopeConfigurationAssignment.properties.filter.resourceTypes.Length -ne $MaintenanceConfigurationDynamicScopingPayload.properties.filter.resourceTypes.Length)
        {
            $isequal = $false
            continue
        }

        foreach ($resourceType in $MaintenanceConfigurationDynamicScopingPayload.properties.filter.resourceTypes)
        {
            if (!($dynamicScopeConfigurationAssignment.properties.filter.resourceTypes -contains $resourceType))
            {
                $isequal = $false
                break
            }
        }

        if ($dynamicScopeConfigurationAssignment.properties.filter.osTypes.Length -ne $MaintenanceConfigurationDynamicScopingPayload.properties.filter.osTypes.Length)
        {
            $isequal = $false
            continue
        }

        foreach ($osType in $MaintenanceConfigurationDynamicScopingPayload.properties.filter.osTypes)
        {
            if (!($dynamicScopeConfigurationAssignment.properties.filter.osTypes -contains $osType))
            {
                $isequal = $false
                break
            }
        }

        if ($dynamicScopeConfigurationAssignment.properties.filter.locations.Length -ne $MaintenanceConfigurationDynamicScopingPayload.properties.filter.locations.Length)
        {
            $isequal = $false
            continue
        }

        foreach ($location in $MaintenanceConfigurationDynamicScopingPayload.properties.filter.locations)
        {
            if (!($dynamicScopeConfigurationAssignment.properties.filter.locations -contains $location))
            {
                $isequal = $false
                break
            }
        }
        
        if ($dynamicScopeConfigurationAssignment.properties.filter.tagSettings.filterOperator -ne $MaintenanceConfigurationDynamicScopingPayload.properties.filter.tagSettings.filterOperator)
        {
            $isequal = $false
            continue
        }

        $maintenanceConfigurationDynamicScopingPayloadTags = @{}
        foreach ($property in $MaintenanceConfigurationDynamicScopingPayload.properties.filter.tagSettings.tags.psobject.Properties)
        {
            $maintenanceConfigurationDynamicScopingPayloadTags[$property.Name] = $property.Value
        }

        $dynamicScopeConfigurationAssignmentTags = @{}
        foreach ($property in $dynamicScopeConfigurationAssignment.properties.filter.tagSettings.tags.psobject.Properties)
        {
            $dynamicScopeConfigurationAssignmentTags[$property.Name] = $property.Value
        }

        foreach ($key in $maintenanceConfigurationDynamicScopingPayloadTags.Keys)
        {
            if (!$dynamicScopeConfigurationAssignmentTags.ContainsKey($key))
            {
                $isequal = $false
                break
            }

            $maintenanceConfigurationDynamicScopingPayloadTagValues = $maintenanceConfigurationDynamicScopingPayloadTags[$key]
            $dynamicScopeConfigurationAssignmentTagValues = $dynamicScopeConfigurationAssignmentTags[$key]

            if ($dynamicScopeConfigurationAssignmentTagValues.Length -ne $maintenanceConfigurationDynamicScopingPayloadTagValues.Length)
            {
                $isequal = $false
                break
            }

            foreach ($value in $maintenanceConfigurationDynamicScopingPayloadTagValues)
            {
                if (!($dynamicScopeConfigurationAssignmentTagValues -contains $value))
                {
                    $isequal = $false
                    break    
                }
            }

            if (!$isequal)
            {
                break
            }
        }

        if ($isequal)
        {
            return $true
        }
    }

    return $false
}

function Set-DynamicScopeForMaintenanceConfiguration
{
    <#
        .SYNOPSIS
            Sets dynamic scope for maintenance configuration.
    
        .DESCRIPTION
            This function sets configuration assignments for dynamic scoping for the azure dynamic queries in software update configuration.
    
        .PARAMETER SoftwareUpdateConfigurationMigrationData
            Software update configration migration data.

        .PARAMETER SoftwareUpdateConfiguration
            Software update configuration

        .PARAMETER Scope
            Scope of dynamic assignment.

        .EXAMPLE
            Set-DynamicScopeForMaintenanceConfiguration -SoftwareUpdateConfigurationMigrationData SoftwareUpdateConfigurationMigrationData -SoftwareUpdateConfiguration SoftwareUpdateConfiguration
    #>
    [CmdletBinding()]
    param 
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [SoftwareUpdateConfigurationMigrationData]$SoftwareUpdateConfigurationMigrationData,

        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        $SoftwareUpdateConfiguration
    )
    
    try
    {
        $argQueryForDynamicScopeConfigurationAssignments = ($ArgDynamicScopeConfigurationAssignmentsClause -f ($SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId))
        $argQueryForDynamicScopeConfigurationAssignments = $QuoteParam + $argQueryForDynamicScopeConfigurationAssignments + $QuoteParam
        $argPayloadObject = @"
        { 
            "subscriptions": [], 
            "query": $argQueryForDynamicScopeConfigurationAssignments
        }
"@
        $argPayload = $argPayloadObject | ConvertTo-Json -Compress -Depth $MaxDepth | ConvertFrom-Json
        $argPath = ("/providers/{0}/{1}" -f $ResourceGraphRP, $ArgResourcesResourceType)
        $records = Invoke-ArmApi-WithPath -Path $argPath -ApiVersion $ResourceGraphApiVersion -Method $POST -Payload $argPayload
        if ($records.Status -eq $Failed)
        {
            Write-Telemetry -Message ("Failed to get dynamic scope config assignments for the maintenance configuration {0} with error code {1} and error message {2}." -f $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId, $records.ErrorCode, $records.ErrorMessage)
        }

        $dynamicConfigurationAssignments = $records.Response.data
    }
    catch [Exception]
    {
        $exceptionMessage = $_.Exception.Message
        Write-Telemetry -Message ("Maintenance configuration [{0}] failed with an unhandled exception: {1} while getting dynamic scope configuration assignments." -f $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId, $exceptionMessage) -Level $ErrorLvl
    }

    $countOfFailedDynamicAssignments = 0

    foreach ($azureQuery in $SoftwareUpdateConfiguration.properties.updateConfiguration.targets.azureQueries)
    {
        foreach ($scope in $azureQuery.scope)
        {
            try
            {
                $maintenanceConfigurationDynamicScopingPayload = ConvertFrom-Json $MaintenanceConfigurationDynamicScopingSettings

                # Figure out if scope is at subscription level or resource group level.
                $parts = $scope.Split("/")
                
                if ($parts.Length -eq 3)
                {
                    # global scope as it is at subscription level.
                    $maintenanceConfigurationDynamicScopingPayload.location = $MaintenanceConfigurationDynamicAssignmentGlobalScope
                }
                else
                {
                    # set scope at resource group.
                    $maintenanceConfigurationDynamicScopingPayload.location = [String]::Empty
                    $maintenanceConfigurationDynamicScopingPayload.properties.filter.resourceGroups = @($parts[4])
                }
    
                $maintenanceConfigurationDynamicScopingPayload.properties.maintenanceConfigurationId = $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId
                $maintenanceConfigurationDynamicScopingPayload.properties.filter.tagSettings = $azureQuery.tagSettings
                $maintenanceConfigurationDynamicScopingPayload.properties.filter.locations = $azureQuery.locations
                $maintenanceConfigurationDynamicScopingPayload.properties.filter.osTypes = @($SoftwareUpdateConfiguration.properties.updateConfiguration.operatingSystem)
                    
                $dynamicScopeConfigurationAlreadyAssigned = Validate-DynamicScopeConfigurationAlreadyAssigned -DynamicScopeConfigurationAssignments $dynamicConfigurationAssignments -MaintenanceConfigurationDynamicScopingPayload $maintenanceConfigurationDynamicScopingPayload -Scope ("/subscriptions/" + $parts[2])

                if (!$dynamicScopeConfigurationAlreadyAssigned)
                {
                    $maintenanceConfigurationDynamicScopingPayload = ConvertTo-Json $maintenanceConfigurationDynamicScopingPayload -Depth $MaxDepth
                    $softwareUpdateConfigurationParts = $SoftwareUpdateConfigurationMigrationData.SoftwareUpdateConfigurationResourceId.Split("/")
    
                    $response = Invoke-ArmApi-WithPath -Path ($MaintenanceConfigurationAssignmentPath -f ( "/subscriptions/" + $parts[2]), $MigrationTag + "_" + $softwareUpdateConfigurationParts[8] + "_" + $softwareUpdateConfigurationParts[10] + (New-Guid).Guid.ToString()) -ApiVersion $MaintenanceConfigurationpApiVersion -Method $PUT -Payload $maintenanceConfigurationDynamicScopingPayload
                    
                    if ($response.Status -eq $Failed)
                    {
                        Write-Telemetry -Message ("Failed to assign dynamic scope {0} to the maintenance configuration {1}." -f $scope, $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId) -Level $ErrorLvl
                        $countOfFailedDynamicAssignments++
                    }
                    else
                    {
                        Write-Telemetry -Message ("Successfuly assigned dynamic scope {0} to the maintenance configuration {1}." -f $scope, $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId)
                    }
    
                }
            }
            catch
            {
                $exceptionMessage = $_.Exception.Message
                Write-Telemetry -Message ("Maintenance configuration [{0}] failed with an unhandled exception: {1} while setting dynamic scope configuration assignment." -f $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId, $exceptionMessage) -Level $ErrorLvl
                $countOfFailedDynamicAssignments++
            }
        }
    }

    if ($countOfFailedDynamicAssignments -eq 0 -and $SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyResolved)
    {
        $SoftwareUpdateConfigurationMigrationData.UpdateDynamicAzureQueriesStatus($SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.HasDynamicAzureQueries, $SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyResolved, $true)
    }
}

function Set-MigrationStatusForSoftwareUpdateConfiguration
{
    <#
        .SYNOPSIS
            Sets migration status for software update configuration.
    
        .DESCRIPTION
            This function sets migration status for software update configuration.
    
        .PARAMETER SoftwareUpdateConfigurationMigrationData
            Software update configration migration data
            
        .EXAMPLE
            Set-MigrationStatusForSoftwareUpdateConfiguration -SoftwareUpdateConfigurationMigrationData SoftwareUpdateConfigurationMigrationData
    #>
    [CmdletBinding()]
    param 
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [SoftwareUpdateConfigurationMigrationData]$SoftwareUpdateConfigurationMigrationData
    )
    
    $migrationStatus = $Migrated
    $errorMessage = [System.Collections.ArrayList]@()

    $machines = $SoftwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration.Keys
    foreach ($machine in $machines)
    {
        if ($Global:Machines[$machine].ResourceType -ne $NonAzureMachine -and $Global:Machines[$machine].ErrorCode -match $NotFoundErrorCode)
        {
            continue
        }

        if ($SoftwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration[$machine].IsConfigAssignmentRequired -and !$SoftwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration[$machine].IsConfigAssignmentSuccessful)
        {
            $migrationStatus = $PartiallyMigrated
            [void]$errorMessage.Add($FailedToCreateMaintenanceConfiguration)
        }

        if ($Global:Machines[$machine].ResourceType -eq $AzureVM -and $Global:Machines[$machine].ScheduledPatchingStatus -ne $Succeeded)
        {
            $migrationStatus = $PartiallyMigrated
            [void]$errorMessage.Add($Global:Machines[$machine].ErrorMessage)
        }
    }

    if ($SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.HasDynamicAzureQueries)
    {
        if (!$SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyResolved)
        {
            $migrationStatus = $PartiallyMigrated 
            [void]$errorMessage.Add($FailedToResolveDynamicAzureQueries)
        }
        if (!$SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.AllDynamicAzureQueriesSuccessfullyAssigned)
        {
            $migrationStatus = $PartiallyMigrated
            [void]$errorMessage.Add($FailedToAssignDynamicAzureQueries)
        }
    }

    if ($SoftwareUpdateConfigurationMigrationData.HasSavedSearchQueries)
    {
        $migrationStatus = $PartiallyMigrated
        [void]$errorMessage.Add($SoftwareUpdateConfigurationHasSavedSearchQueries)
    }

    $SoftwareUpdateConfigurationMigrationData.MigrationStatus = $migrationStatus
    $SoftwareUpdateConfigurationMigrationData.ErrorMessage = $errorMessage
}

function Get-WindowsTimeZoneFromIANATimeZone
{
    <#
        .SYNOPSIS
            Takes an IANA time zone and converts it to a Windows time zone.
        
        .DESCRIPTION
            IANA time zones are returned by most web endpoints. We can translate any
            given IANA time zone (e.g. America/New_York) into a proper Windows time zone
            (e.g. Eastern Standard Time).
   
            This function leverages the unicode.org windowsZones.xml file to translate
            IANA to Windows time zones.
            Github repo link: https://github.com/unicode-org/cldr
    
        .EXAMPLE
            Get-WindowsTimeZoneFromIANATimeZone "America/New_York"
            Returns the Windows time zone "Eastern Standard Time"
            
        .PARAMETER IANATimeZone
            IANA time zone string.
            Reference: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    #>
    [CmdletBinding()]
    param 
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]        
        [string]$IANATimeZone
    )
    try 
    {
        
        $url = "https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml"
        $xml = [xml]((Invoke-WebRequest -Uri $url -ContentType 'application/xml' -UseBasicParsing).Content)
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Failed to obtain time zone XML map from GitHub with exception {0}." -f $_.Exception) -Level $ErrorLvl
        throw
    }

    $win_tz = ($xml.supplementalData.windowsZones.mapTimezones.mapZone | Where-Object type -Match $IANATimeZone | Select-Object -First 1).other
    return $win_tz
}

function Create-MaintenanceConfigurationFromSoftwareUpdateConfiguration 
{
    <#
        .SYNOPSIS
            Creates maintenance configuration for the software update configuration.
    
        .DESCRIPTION
            This function creates equivalent maintenance configuration for the software update configuration.
    
        .PARAMETER SoftwareUpdateConfigurationMigrationData
            Software update configration migration data
        
        .PARAMETER SoftwareUpdateConfiguration
            Software update configuration

        .PARAMETER AutomationAccountResourceId
            Automation Account Resource Id
    
        .EXAMPLE
            Create-MaintenanceConfigurationFromSoftwareUpdateConfiguration -SoftwareUpdateConfigurationMigrationData SoftwareUpdateConfigurationMigrationData -SoftwareUpdateConfiguration SoftwareUpdateConfiguration -AutomationAccountResourceId AaId
    #>
    [CmdletBinding()]
    param 
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        [SoftwareUpdateConfigurationMigrationData]$SoftwareUpdateConfigurationMigrationData,
    
        [Parameter(Mandatory = $true, Position = 2, ValueFromPipeline = $true)]
        $SoftwareUpdateConfiguration,

        [Parameter(Mandatory = $true, Position = 3)]
        [String]$AutomationAccountResourceId
    )

    try
    {
        $parts = $SoftwareUpdateConfiguration.id.Split("/");

        $maintenanceConfigurationResourceId = "/subscriptions/" + $parts[2] + "/resourceGroups/" + $ResourceGroupNameForMaintenanceConfigurations + "/providers/Microsoft.Maintenance/maintenanceConfigurations/" + $parts[10]
        $maintenanceConfigurationPayload = ConvertFrom-Json $MaintenanceConfigurationSettings
    
        $maintenanceConfigurationPayload.location = $Global:AutomationAccountRegion
    
        # Convert from IANA Time Zone ID to Windows/System Time Zone ID and Set start and expiration times of maintenance configuration.
        $softwareUpdateConfigurationTimeZone = if ($TimeZonesBackwardCompatability.ContainsKey($SoftwareUpdateConfiguration.properties.scheduleInfo.timeZone)) { $TimeZonesBackwardCompatability[$SoftwareUpdateConfiguration.properties.scheduleInfo.timeZone] } else { $SoftwareUpdateConfiguration.properties.scheduleInfo.timeZone }
        $systemTimeZone = Get-WindowsTimeZoneFromIANATimeZone -IANATimeZone $softwareUpdateConfigurationTimeZone
        $maintenanceConfigurationPayload.properties.maintenanceWindow.timeZone = $systemTimeZone
        $maintenanceConfigurationPayload.properties.maintenanceWindow.startDateTime = Get-Date -Date ([System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]$SoftwareUpdateConfiguration.properties.scheduleInfo.nextRun, $systemTimeZone)) -Format $MRPScheduleDateFormat
    
        if ($null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.expiryTime)
        {    
            # The start time and expiration time for software update configurations can be in the format yyyy-mm-DDTHH:mm:ss+01:00 or yyyy-mm-DDTHH:mm:ss-04:30. This is indicative of the time in time zone and UTC offset.
            # MRP accepts start time and expiration time only in the format yyyy-mm-DD HH:mm.    
            $maintenanceConfigurationPayload.properties.maintenanceWindow.expirationDateTime = Get-Date -Date ([System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]$SoftwareUpdateConfiguration.properties.scheduleInfo.expiryTime, $systemTimeZone)) -Format $MRPScheduleDateFormat            
        }
    
        # Set duration of maintenance configuration.
        $duration = [Xml.XmlConvert]::ToTimeSpan($SoftwareUpdateConfiguration.properties.updateConfiguration.duration)
        if ($duration.Hours -le 1)
        {
            $maintenanceConfigurationPayload.properties.maintenanceWindow.duration = $MRPScheduleMinAllowedDuration
        }
        elseif ($duration.Hours -ige 4 -and $duration.Minutes -ige 0)
        {
            $maintenanceConfigurationPayload.properties.maintenanceWindow.duration = $MRPScheduleMaxAllowedDuration
        }
        else
        {
            $maintenanceConfigurationPayload.properties.maintenanceWindow.duration = $duration.ToString("hh\:mm")
        }
    
        # Set recurrence of maintenance configuration.
        $recur = ""
        # Handle One-Time Software Update Configurations
        if ($SoftwareUpdateConfiguration.properties.scheduleInfo.frequency -eq $OneTime)
        {
            # Specify the minimum recurrence.
            $recur = "6" + $Hours
    
            # Set expiration time from start time to maintenance window duration plus one hour. Maintenance window max can be 4 hours only.
            $maintenanceConfigurationPayload.properties.maintenanceWindow.expirationDateTime = Get-Date -Date ([System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]$SoftwareUpdateConfiguration.properties.scheduleInfo.nextRun, $systemTimeZone)).AddHours($duration.Hours).AddMinutes($duration.Minutes).AddHours(1) -Format $MRPScheduleDateFormat
        }
        elseif ($SoftwareUpdateConfiguration.properties.scheduleInfo.frequency -eq $Hour)
        {
            $interval = ""
            
            if ($SoftwareUpdateConfiguration.properties.scheduleInfo.interval -lt 6)
            {
                $interval = "6" # Minimum hourly supported schedules in MRP.
                $recur = $interval.ToString() + $Hours

            }
            elseif ($SoftwareUpdateConfiguration.properties.scheduleInfo.interval -gt 35)
            {
                $interval = [System.Math]::Round(($SoftwareUpdateConfiguration.properties.scheduleInfo.interval)/24) # Maximum hourly supported schedules in MRP is 35. Translate to days.
                $recur = $interval.ToString() + $Days
            }
            else
            {
                $interval = $SoftwareUpdateConfiguration.properties.scheduleInfo.interval
                $recur = $interval.ToString() + $Hours
            }    
        }
        elseif ($SoftwareUpdateConfiguration.properties.scheduleInfo.frequency -eq $Day)
        {
            if ($SoftwareUpdateConfiguration.properties.scheduleInfo.interval -gt 35)
            {
                $interval = [System.Math]::Round(($SoftwareUpdateConfiguration.properties.scheduleInfo.interval)/7) # Maximum Daily supported schedules in MRP is 35. Translate to weeks.
                $recur = $interval.ToString() + $Weeks
            }
            else
            {
                $interval = $SoftwareUpdateConfiguration.properties.scheduleInfo.interval
                $recur = $interval.ToString() + $Days
            }                
        }
        elseif ($SoftwareUpdateConfiguration.properties.scheduleInfo.frequency -eq $Week)
        {
            if ($SoftwareUpdateConfiguration.properties.scheduleInfo.interval -gt 35)
            {
                $interval = [System.Math]::Round(($SoftwareUpdateConfiguration.properties.scheduleInfo.interval)/4.34)  # Maximum Weekly supported schedules in MRP is 35. Translate to Months.
                $recur = $interval.ToString() + $Months
                # Default day needs to be specified else API throws BadRequest.
                $recur = $recur + " " + $Day.ToLower() + "1"
            }
            else
            {
                $interval = $SoftwareUpdateConfiguration.properties.scheduleInfo.interval
                $recur = $interval.ToString() + $Weeks
            
                if ($null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule -and $null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.weekDays)
                {
                    $recur = $recur + " "
                    for ($i = 0; $i -lt $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.weekDays.Length; $i++)
                    {
                        $recur = $recur + $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.weekDays[$i]
                        if ($i -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.weekDays.Length -1)
                        {
                            $recur = $recur + ","
                        }
                    }
                }    
            }    
        }
        elseif ($SoftwareUpdateConfiguration.properties.scheduleInfo.frequency -eq $Month)
        {
            if ($SoftwareUpdateConfiguration.properties.scheduleInfo.interval -gt 35)
            {
                $interval = "35" # Maximum Monthly supported schedules in MRP is 35.
            }
            else
            {
                $interval = $SoftwareUpdateConfiguration.properties.scheduleInfo.interval
            }
    
            $recur = $interval.ToString() + $Months
    
            if ($null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule -and $null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthDays)
            {
                $recur = $recur + " "
                for ($i = 0; $i -lt $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthDays.Length; $i++)
                {
                    if ($SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthDays[$i] -eq -1)
                    {
                        # To run on the last day of the month.
                        $recur = $recur + $Day.ToLower() + "-1"
                    }
                    else
                    {
                        $recur = $recur + $Day.ToLower() + $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthDays[$i]
                    }
                    if ($i -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthDays.Length -1)
                    {
                        $recur = $recur + ","
                    }
                }
            }
            elseif ($null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule -and $null -ne $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthlyOccurrences)
            {
                $recur = $recur + " " + $SoftwareUpdateConfigurationToMaintenanceConfigurationPatchDaySetting[$SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthlyOccurrences[0].occurrence.ToString()] + " " + $SoftwareUpdateConfiguration.properties.scheduleInfo.advancedSchedule.monthlyOccurrences[0].day
            }
            else
            {
                # Default day needs to be specified else API throws BadRequest.
                $recur = $recur + " " + $Day.ToLower() + "1"
            }
        }
        $maintenanceConfigurationPayload.properties.maintenanceWindow.recurEvery = $recur
    
        # Set install patches properties of maintenance configuration.
        if ($SoftwareUpdateConfiguration.properties.updateConfiguration.operatingSystem -eq $Windows)
        {
            $maintenanceConfigurationPayload.properties.installPatches.linuxParameters = $null

            $classifications = [System.Collections.ArrayList]@()

            foreach ($category in $SoftwareUpdateConfiguration.properties.updateConfiguration.windows.includedUpdateClassifications.replace(" ","").Split(","))
            {
                [void]$classifications.Add($category)                
            }

            $maintenanceConfigurationPayload.properties.installPatches.windowsParameters.classificationsToInclude = $classifications
            $maintenanceConfigurationPayload.properties.installPatches.windowsParameters.kbNumbersToInclude = $SoftwareUpdateConfiguration.properties.updateConfiguration.windows.includedKbNumbers
            $maintenanceConfigurationPayload.properties.installPatches.windowsParameters.kbNumbersToExclude = $SoftwareUpdateConfiguration.properties.updateConfiguration.windows.excludedKbNumbers
            $maintenanceConfigurationPayload.properties.installPatches.rebootSetting = $SoftwareUpdateConfigurationToMaintenanceConfigurationRebootSetting[$SoftwareUpdateConfiguration.properties.updateConfiguration.windows.rebootSetting]
        }
        else
        {
            $maintenanceConfigurationPayload.properties.installPatches.windowsParameters = $null

            $classifications = [System.Collections.ArrayList]@()

            foreach ($category in $SoftwareUpdateConfiguration.properties.updateConfiguration.linux.includedPackageClassifications.replace(" ","").Split(","))
            {
                [void]$classifications.Add($category)                
            }
            
            $maintenanceConfigurationPayload.properties.installPatches.linuxParameters.classificationsToInclude = $classifications
            $maintenanceConfigurationPayload.properties.installPatches.linuxParameters.packageNameMasksToInclude = $SoftwareUpdateConfiguration.properties.updateConfiguration.linux.includedPackageNameMasks
            $maintenanceConfigurationPayload.properties.installPatches.linuxParameters.packageNameMasksToExclude = $SoftwareUpdateConfiguration.properties.updateConfiguration.linux.excludedPackageNameMasks
            $maintenanceConfigurationPayload.properties.installPatches.rebootSetting = $SoftwareUpdateConfigurationToMaintenanceConfigurationRebootSetting[$SoftwareUpdateConfiguration.properties.updateConfiguration.linux.rebootSetting]
        }
        
        $maintenanceConfigurationPayload = ConvertTo-Json $maintenanceConfigurationPayload -Depth $MaxDepth
    
        $response = Invoke-ArmApi-WithPath -Path $maintenanceConfigurationResourceId -ApiVersion $MaintenanceConfigurationpApiVersion -Method $PUT -Payload $maintenanceConfigurationPayload
    
        if ($response.Status -eq $Failed)
        {
            Write-Telemetry -Message ("Failed to Create maintenance configuration for the software update configuration {0}." -f $SoftwareUpdateConfiguration.id ) -Level $ErrorLvl
            $SoftwareUpdateConfigurationMigrationData.MigrationStatus = $MigrationFailed
            [void]$SoftwareUpdateConfigurationMigrationData.ErrorMessage.Add($FailedToCreateMaintenanceConfiguration)
            return $SoftwareUpdateConfigurationMigrationData
        }
    
        Write-Telemetry -Message ("Maintenance configuration {0} created for software update configuration {1}." -f $maintenanceConfigurationResourceId, $SoftwareUpdateConfiguration.id)
        $SoftwareUpdateConfigurationMigrationData.MaintenanceConfigurationResourceId = $maintenanceConfigurationResourceId    
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Failed to create maintenance configuration for software update configuration {0} with unhandled exception {1}." -f $SoftwareUpdateConfiguration.id, $_.Exception.Message) -Level $ErrorLvl
        $SoftwareUpdateConfigurationMigrationData.MigrationStatus = $MigrationFailed
        [void]$SoftwareUpdateConfigurationMigrationData.ErrorMessage.Add($FailedToCreateMaintenanceConfiguration + "because of unhandled exception" + $_.Exception.Message)
        return $SoftwareUpdateConfigurationMigrationData
    }

    Set-MaintenanceConfigurationAssignmentForMachines -SoftwareUpdateConfigurationMigrationData $SoftwareUpdateConfigurationMigrationData
    
    if ($SoftwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.HasDynamicAzureQueries)
    {
        Set-DynamicScopeForMaintenanceConfiguration -SoftwareUpdateConfigurationMigrationData $SoftwareUpdateConfigurationMigrationData -SoftwareUpdateConfiguration $SoftwareUpdateConfiguration
    }

    Set-MigrationStatusForSoftwareUpdateConfiguration -SoftwareUpdateConfigurationMigrationData $SoftwareUpdateConfigurationMigrationData

    if ($SoftwareUpdateConfigurationMigrationData.MigrationStatus -eq $Migrated)
    {
        $schedules = $Global:JobSchedules[$SoftwareUpdateConfiguration.name]
        foreach ($schedule in $schedules)
        {
            Disable-SoftwareUpdateConfiguration -AutomationAccountResourceId $AutomationAccountResourceId -ScheduleName $schedule -SoftwareUpdateConfigurationMigrationData $SoftwareUpdateConfigurationMigrationData
        }
    }

    return $SoftwareUpdateConfigurationMigrationData
}

function Create-ResourceGroupForMaintenanceConfigurations
{
   <#
        .SYNOPSIS
            Create resource group for maintenance configurations.
    
        .DESCRIPTION
            This command will create resource group for maintenance configurations in the same subscription and region as the automation account.

        .PARAMETER AutomationAccountResourceId
            Automation Account Id.
        
        .PARAMETER ResourceGroupNameForMaintenanceConfigurations
            Resource group for maintenance configurations.

        .EXAMPLE
            Create-ResourceGroupForMaintenanceConfigurations -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}" -ResourceGroupNameForMaintenanceConfigurations "rgName"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId,

        [Parameter(Mandatory = $true, Position = 2)]
        [String]$ResourceGroupNameForMaintenanceConfigurations
    )
    
    $response = Invoke-ArmApi-WithPath -Path $AutomationAccountResourceId -ApiVersion $AutomationAccountApiVersion -Method $GET
    $Global:AutomationAccountRegion = $response.Response.location

    $resourceGroupPayload = ConvertFrom-Json $ResourceGroupPayload
    $resourceGroupPayload.location = $Global:AutomationAccountRegion
    $resourceGroupPayload = ConvertTo-Json $resourceGroupPayload -Depth $MaxDepth

    $parts = $AutomationAccountResourceId.Split("/")

    #Check if resource group already exists.

    $response = Invoke-ArmApi-WithPath -Path ($ResourceGroupPath -f ("/subscriptions/" + $parts[2]), $ResourceGroupNameForMaintenanceConfigurations) -ApiVersion $ResourceGroupApiVersion -Method $GET

    if ($null -ne $response.Response.id)
    {
        Write-Telemetry -Message ("Resource group {0} already exists." -f $response.Response.id)
        return
    }
    
    $response = Invoke-ArmApi-WithPath -Path ($ResourceGroupPath -f ("/subscriptions/" + $parts[2]), $ResourceGroupNameForMaintenanceConfigurations) -ApiVersion $ResourceGroupApiVersion -Method $PUT -Payload $resourceGroupPayload

    if ($null -eq $response.Response.id)
    {
        Write-Telemetry -Message ("Failed to create resource group with error code {0} and error message {1}." -f $response.ErrorCode, $response.ErrorMessage) -Level $ErrorLvl
        throw
    }
    else
    {
        Write-Telemetry -Message ("Resource group {0} created successfully." -f $response.Response.id)
    }
}

function Get-AllSoftwareUpdateConfigurations
{
    <#
        .SYNOPSIS
            Gets all software update configurations.
    
        .DESCRIPTION
            This function gets all software update configurations with support for pagination.
    
        .PARAMETER AutomationAccountResourceId
            Automation account resource id.
            
        .EXAMPLE
            Get-AllSoftwareUpdateConfigurations -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId
    )
    $output = $null
    $skip = 0
    do
    {
        $path = ($SoftwareUpdateConfigurationsPath -f $AutomationAccountResourceId, $skip)
        $output = Invoke-ArmApi-WithPath -Path $path -ApiVersion $SoftwareUpdateConfigurationApiVersion -Method $GET
        if($output.Status -eq $Failed)
        {
            Write-Telemetry -Message ("Failed to get software update configurations with error code {0} and error message {1}." -f $output.ErrorCode, $output.ErrorMessage)
            throw
        }
        foreach ($result in $output.Response.value)
        {
            if (!$Global:SoftwareUpdateConfigurationsResourceIDs.ContainsKey($result.id))
            {
                $Global:SoftwareUpdateConfigurationsResourceIDs[$result.id] = $result.name
            }
        }
        # API paginates in multiples of 100.
        $skip = $skip + 100
    }
    while ($null -ne $output.Response.nextLink);
}

function Migrate-AllSoftwareUpdateConfigurationsToMaintenanceConfigurations
{
    <#
        .SYNOPSIS
            Starts migration job for software update configurations.
    
        .DESCRIPTION
            This function starts the migration of software update configurations to maintenance configurations.
    
        .PARAMETER AutomationAccountResourceId
            Automation account resource id.

        .PARAMETER ResourceGroupNameForMaintenanceConfigurations
            Resource group for maintenance configurations.
            
        .EXAMPLE
            Migrate-AllSoftwareUpdateConfigurationToMaintenanceConfiguration -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}" -ResourceGroupNameForMaintenanceConfigurations "rgName"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId,

        [Parameter(Mandatory = $true, Position = 2)]
        [String]$ResourceGroupNameForMaintenanceConfigurations
    )

    try
    {
        Create-ResourceGroupForMaintenanceConfigurations -ResourceGroupNameForMaintenanceConfigurations $ResourceGroupNameForMaintenanceConfigurations -AutomationAccountResourceId $AutomationAccountResourceId

        Initialize-JobSchedules -AutomationAccountResourceId $AutomationAccountResourceId
    
        $allSoftwareUpdateConfigurationsMigrationData = @{}
        Get-AllSoftwareUpdateConfigurations -AutomationAccountResourceId $AutomationAccountResourceId
     
        $softwareUpdateConfigurations = [System.Collections.ArrayList]@($Global:SoftwareUpdateConfigurationsResourceIDs.Keys)
        foreach ($softwareUpdateConfiguration in $softwareUpdateConfigurations)
        {            
            $softwareUpdateConfigurationMigrationData = Migrate-SoftwareUpdateConfigurationToMaintenanceConfiguration -SoftwareUpdateConfigurationId $softwareUpdateConfiguration -AutomationAccountResourceId $AutomationAccountResourceId
            $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData.SoftwareUpdateConfigurationResourceId] = $softwareUpdateConfigurationMigrationData                
        }
    
        $softwareUpdateConfigurationsMigrationData = $allSoftwareUpdateConfigurationsMigrationData.Keys
        foreach ($softwareUpdateConfigurationMigrationData in $softwareUpdateConfigurationsMigrationData)
        {
            Write-Telemetry -Message ("Migration status for software update configuration `n {0}" -f ($allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData] | ConvertTo-Json -Depth $MaxDepth))
        }

        $machines = [System.Collections.ArrayList]@($Global:Machines.Keys)
        
        $countOfMachinesRetrievedFromAzureDynamicQueriesWhichAreNotOnboardedToAutomationUpdateManagement = $Global:MachinesRetrievedFromAzureDynamicQueriesWhichAreNotOnboardedToAutomationUpdateManagement
        $countOfAzureMachinesWithPeriodicAssessmentEnabled = 0
        $countOfAzuresMachinesWithPatchSettingsEnabled = 0
        $countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled = 0
        $countOfAzureMachinesWhereFailedToEnablePeriodicAssessment = 0
        $countOfAzureMachinesWhereFailedToEnablePatchSettings = 0
        $countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment = 0
        $countOfNotFoundMachines = 0

        $countOfAllSoftwareUpdateConfigurations = $softwareUpdateConfigurations.Count
        $countOfMigratedSoftwareUpdateConfigurations = 0
        $countOfPartiallyMigratedSoftwareUpdateConfigurations = 0
        $countOfNotMigratedSoftwareUpdateConfigurations = 0
        $countOfFailedToMigrateSoftwareUpdateConfigurations = 0
        $countOfDisabledSoftwareUpdateConfigurations = 0

        foreach ($machine in $machines)
        {
            if ($Global:Machines[$machine].ResourceType -ne $NonAzureMachine -and $Global:Machines[$machine].ErrorCode -match $NotFoundErrorCode)
            {
                $countOfNotFoundMachines++
            }
            elseif ($Global:Machines[$machine].ResourceType -eq $AzureVM)
            {
                if ($Global:Machines[$machine].PeriodicAssessmentStatus -eq $Succeeded)
                {
                    $countOfAzureMachinesWithPeriodicAssessmentEnabled++
                }
                elseif ($Global:Machines[$machine].PeriodicAssessmentStatus -eq $Failed)
                {
                    $countOfAzureMachinesWhereFailedToEnablePeriodicAssessment++
                }

                if ($Global:Machines[$machine].ScheduledPatchingStatus -eq $Succeeded)
                {
                    $countOfAzuresMachinesWithPatchSettingsEnabled++
                }
                elseif ($Global:Machines[$machine].ScheduledPatchingStatus -eq $Failed)
                {
                    $countOfAzureMachinesWhereFailedToEnablePatchSettings++
                }
            }
            elseif ($Global:Machines[$machine].ResourceType -eq $ArcServer)
            {
                if ($Global:Machines[$machine].PeriodicAssessmentStatus -eq $Succeeded)
                {
                    $countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled++
                }
                elseif ($Global:Machines[$machine].PeriodicAssessmentStatus -eq $Failed)
                {
                    $countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment++
                }
            }                
        }

        foreach ($softwareUpdateConfigurationMigrationData in $softwareUpdateConfigurationsMigrationData)
        {
            if ($allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MigrationStatus -eq $PartiallyMigrated)
            {
                Write-Output ("Software update configuration {0} is partially migrated. Corressponding maintenance configuration resource id is {1}. Please refer to verbose logs for details." -f $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].SoftwareUpdateConfigurationResourceId, $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MaintenanceConfigurationResourceId)
                $countOfPartiallyMigratedSoftwareUpdateConfigurations++
            }
            elseif ($allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MigrationStatus -eq $Migrated)
            {
                if ($allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].UnderlyingScheduleDisabled)
                {
                    $countOfDisabledSoftwareUpdateConfigurations++
                    Write-Output ("Software update configuration {0} is migrated. Corressponding maintenance configuration resource id is {1}. Underlying software update configration schedule is disabled." -f $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].SoftwareUpdateConfigurationResourceId, $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MaintenanceConfigurationResourceId)
                }
                else
                {
                    Write-Output ("Software update configuration {0} is migrated. Corressponding maintenance configuration resource id is {1}. Underlying software update configration schedule is not disabled. Please refer to verbose logs for details." -f $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].SoftwareUpdateConfigurationResourceId, $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MaintenanceConfigurationResourceId)                    
                }
                $countOfMigratedSoftwareUpdateConfigurations++
            }
            elseif ($allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MigrationStatus -eq $NotMigrated)
            {
                Write-Output ("Software update configuration {0} is not migrated. Please refer to verbose logs for details." -f $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].SoftwareUpdateConfigurationResourceId)
                $countOfNotMigratedSoftwareUpdateConfigurations++
            }
            elseif ($allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].MigrationStatus -eq $MigrationFailed)
            {
                Write-Output ("Failed to migrate software update configuration {0}. Please refer to verbose logs for details." -f $allSoftwareUpdateConfigurationsMigrationData[$softwareUpdateConfigurationMigrationData].SoftwareUpdateConfigurationResourceId)
                $countOfFailedToMigrateSoftwareUpdateConfigurations++
            }
        }

        if ($countOfMachinesRetrievedFromAzureDynamicQueriesWhichAreNotOnboardedToAutomationUpdateManagement -gt 0)
        {
            Write-Output ("Total {0} azure machines retrieved from azure dynamic queries which are not onboarded to automation update management." -f $countOfMachinesRetrievedFromAzureDynamicQueriesWhichAreNotOnboardedToAutomationUpdateManagement)
        }
        if ($countOfAzureMachinesWithPeriodicAssessmentEnabled -gt 0)
        {
            Write-Output ("{0} azure machines linked to software update configurations enabled for periodic assessment." -f $countOfAzureMachinesWithPeriodicAssessmentEnabled)
        }
        if ($countOfAzureMachinesWhereFailedToEnablePeriodicAssessment -gt 0)
        {
            Write-Output ("Failed to enable periodic assessment for {0} azure machines linked to software update configurations." -f $countOfAzureMachinesWhereFailedToEnablePeriodicAssessment)
        }
        if ($countOfAzuresMachinesWithPatchSettingsEnabled -gt 0)
        {
            Write-Output ("{0} azure machines linked to software update configurations enabled for scheduled patching." -f $countOfAzuresMachinesWithPatchSettingsEnabled)
        }
        if ($countOfAzureMachinesWhereFailedToEnablePatchSettings -gt 0)
        {
            Write-Output ("Failed to enable patch settings for {0} azure machines linked to software update configurations." -f $countOfAzureMachinesWhereFailedToEnablePatchSettings)
        }
        if ($countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled -gt 0)
        {
            Write-Output ("{0} non-azure arc machines linked to software update configurations enabled for periodic assessment." -f $countOfNonAzureArcMachinesWithPeriodicAssessmentEnabled)
        }
        if ($countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment -gt 0)
        {
            Write-Output ("Failed to enable periodic assessment for {0} non-azure arc machines linked to software update configurations." -f $countOfNonAzureArcMachinesWhereFailedToEnablePeriodicAssessment)
        }
        if ($countOfNotFoundMachines -gt 0)
        {
            Write-Output ("{0} azure/arc machines linked to software update configurations are deleted and not found." -f $countOfNotFoundMachines)
        }

        Write-Output ("Total {0} software update configurations found under automation account {1}." -f $countOfAllSoftwareUpdateConfigurations, $AutomationAccountResourceId)
        if ($countOfMigratedSoftwareUpdateConfigurations -gt 0)
        {
            Write-Output ("{0} software update configurations migrated." -f $countOfMigratedSoftwareUpdateConfigurations)
        }
        if ($countOfDisabledSoftwareUpdateConfigurations -gt 0)
        {
            Write-Output ("{0} software update configurations disabled." -f $countOfDisabledSoftwareUpdateConfigurations)
        }
        if ($countOfPartiallyMigratedSoftwareUpdateConfigurations -gt 0)
        {
            Write-Output ("{0} software update configurations partially migrated." -f $countOfPartiallyMigratedSoftwareUpdateConfigurations)
        }
        if ($countOfNotMigratedSoftwareUpdateConfigurations -gt 0)
        {
            Write-Output ("{0} software update configurations not migrated." -f $countOfNotMigratedSoftwareUpdateConfigurations)
        }
        if ($countOfFailedToMigrateSoftwareUpdateConfigurations -gt 0)
        {
            Write-Output ("Failed to migrate {0} software update configurations." -f $countOfFailedToMigrateSoftwareUpdateConfigurations)
        }
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Unhandled Exception {0}." -f $_.Exception.Message) -Level $ErrorLvl
    }
}

function Migrate-SoftwareUpdateConfigurationToMaintenanceConfiguration
{
   <#
        .SYNOPSIS
            Creates equivalent MRP maintenance configuration from software update configuration.
    
        .DESCRIPTION
            This command will
            1. Set assessment mode properties and patch mode properties for azure/arc-onboarded machines attached to software update configuration or picked up through dynamic queries.
            2. Create maintenance configuration.
            3. Config assigements for machines attached to software update configuration.
            4. Dynamic scoping for maintenance configuration.
    
        .PARAMETER SoftwareUpdateConfigurationId
            Software update configuration Id.
        
        .PARAMETER AutomationAccountResourceId
            Automation Account Resource Id.
                
        .EXAMPLE
            Migrate-SoftwareUpdateConfigurationToMaintenanceConfiguration -SoftwareUpdateConfigurationId SoftwareUpdateConfigurationId -AutomationAccountResourceId "/subscriptions/{sub}/../accounts/{aaName}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1, ValueFromPipeline = $true)]
        $SoftwareUpdateConfigurationId,

        [Parameter(Mandatory = $true, Position = 2)]
        [String]$AutomationAccountResourceId
    )

    $softwareUpdateConfigurationMigrationData = [SoftwareUpdateConfigurationMigrationData]::new()

    try
    {
        $response = Invoke-ArmApi-WithPath -Path $SoftwareUpdateConfigurationId -ApiVersion $SoftwareUpdateConfigurationApiVersion -Method $GET

        if ($response.Status -eq $Failed)
        {
            Write-Telemetry -Message ("Failed to get software update configuration {0} with error code {1} and error message {2}." -f $SoftwareUpdateConfigurationId, $response.ErrorCode, $response.ErrorMessage) -Level $ErrorLvl
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($response.ErrorMessage)
            return $softwareUpdateConfigurationMigrationData
        }

        $softwareUpdateConfiguration = $response.Response

        $softwareUpdateConfigurationMigrationData.SoftwareUpdateConfigurationResourceId = $softwareUpdateConfiguration.id
        $softwareUpdateConfigurationMigrationData.OperatingSystem = $softwareUpdateConfiguration.properties.updateConfiguration.operatingSystem

        if (($softwareUpdateConfigurationMigrationData.OperatingSystem -eq $Windows -and $properties.updateConfiguration.windows.rebootSetting -eq $RebootOnly) -or 
            ($softwareUpdateConfigurationMigrationData.OperatingSystem -eq $Linux -and $properties.updateConfiguration.linux.rebootSetting -eq $RebootOnly))
        {
            Write-Telemetry -Message ("Skipping software update configuration {0} as reboot setting is set to reboot only. " -f $softwareUpdateConfiguration.id ) -Level $Warning
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($SoftwareUpdateConfigurationHasRebootOnlySetting)
            return $softwareUpdateConfigurationMigrationData
        }
    
        if (($null -ne $softwareUpdateConfiguration.properties.tasks.preTask) -or ($null -ne $softwareUpdateConfiguration.properties.tasks.postTask))
        {
            Write-Telemetry -Message ("Skipping software update configuration {0} as it has pre/post tasks. " -f $softwareUpdateConfiguration.id ) -Level $Warning
            $softwareUpdateConfigurationMigrationData.HasPrePostTasks = $true
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($SoftwareUpdateConfigurationHasPrePostTasks)
            return $softwareUpdateConfigurationMigrationData

        }
        else
        {
            $softwareUpdateConfigurationMigrationData.HasPrePostTasks = $false
        }
    
        if ($null -ne $softwareUpdateConfiguration.properties.updateConfiguration.targets.nonAzureQueries -and $softwareUpdateConfiguration.properties.updateConfiguration.targets.nonAzureQueries.Count -gt 0)
        {
            $softwareUpdateConfigurationMigrationData.HasSavedSearchQueries = $true
        }
        else
        {
            $softwareUpdateConfigurationMigrationData.HasSavedSearchQueries = $false
        }
    
        if ($null -ne $softwareUpdateConfiguration.properties.updateConfiguration.targets.azureQueries -and $softwareUpdateConfiguration.properties.updateConfiguration.targets.azureQueries.Count -gt 0)
        {
            $softwareUpdateConfigurationMigrationData.UpdateDynamicAzureQueriesStatus($true, $false, $false)
        }
        else
        {
            $softwareUpdateConfigurationMigrationData.UpdateDynamicAzureQueriesStatus($false, $false, $false)
        }
        
        if ($softwareUpdateConfiguration.properties.provisioningState -ne $SoftwareUpdateConfigurationSucceededProvisioningState)
        {
            Write-Telemetry -Message ("Skipping software update configuration {0} as provisioning state is not succeeded. " -f $softwareUpdateConfiguration.id ) -Level $Warning
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($SoftwareUpdateConfigurationNotProvisionedSuccessfully)
            return $softwareUpdateConfigurationMigrationData
        }
    
        if (![String]::IsNullOrEmpty($softwareUpdateConfiguration.properties.error))
        {
            Write-Telemetry -Message ("Skipping software update configuration {0} as its in errored state. " -f $softwareUpdateConfiguration.id ) -Level $Warning
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($SoftwareUpdateConfigurationInErroredState)
            return $softwareUpdateConfigurationMigrationData
        }
    
        # Expiration time is already in the past and no further runs for the software update configuration.
        if ([String]::IsNullOrEmpty($softwareUpdateConfiguration.properties.scheduleInfo.nextRun))
        {
            Write-Telemetry -Message ("Skipping software update configuration {0} as it is an already expired schedule. " -f $softwareUpdateConfiguration.id ) -Level $Warning
            $softwareUpdateConfigurationMigrationData.UnderlyingScheduleDisabled = $true
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($ExpiredSoftwareUpdateConfigurationMessage)
            return $softwareUpdateConfigurationMigrationData
        }
    
        # Underlying schedule for the software update configuration is disabled.
        if (!$softwareUpdateConfiguration.properties.scheduleInfo.isEnabled)
        {
            Write-Telemetry -Message ("Skipping software update configuration {0} as underlying schedule is disabled. " -f $softwareUpdateConfiguration.id ) -Level $Warning
            $softwareUpdateConfigurationMigrationData.UnderlyingScheduleDisabled = $true
            $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
            [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($DisabledSoftwareUpdateConfigurationMessage)
            return $softwareUpdateConfigurationMigrationData
        }
            
        # Start of Getting Machines Ready for Onboarding to Azure Update Manager.
        $azureVirtualMachines = $softwareUpdateConfiguration.properties.updateConfiguration.azureVirtualMachines
        $nonAzureComputerNames = $softwareUpdateConfiguration.properties.updateConfiguration.nonAzureComputerNames
        
        foreach ($azureVirtualMachine in $azureVirtualMachines)
        {
            if ($Global:Machines.ContainsKey($azureVirtualMachine))
            {
                $softwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration($azureVirtualMachine, $true, $false, $false, [String]::Empty, [String]::Empty)
            }
        }
    
        foreach ($nonAzureComputerName in $nonAzureComputerNames)
        {
            if ($Global:Machines.ContainsKey($nonAzureComputerName))
            {
                $softwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration($nonAzureComputerName, $true, $false, $false, [String]::Empty, [String]::Empty)
            }
        }
    
        if ($softwareUpdateConfigurationMigrationData.DynamicAzureQueriesStatus.HasDynamicAzureQueries)
        {
            $azureVirtualMachinesResolvedThroughDynamicQueries = Get-MachinesFromAzureDynamicQueries -SoftwareUpdateConfigurationMigrationData $softwareUpdateConfigurationMigrationData -AzureQueries $softwareUpdateConfiguration.properties.updateConfiguration.targets.azureQueries
    
            foreach ($azureVirtualMachine in $azureVirtualMachinesResolvedThroughDynamicQueries)
            {
        
                # If machine is already attached to software update configuration and also picked through dynamic query.
                if ($softwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration.ContainsKey($azureVirtualMachine))
                {
                    $machineReadinessDataForSoftwareUpdateConfiguration = $softwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration[$azureVirtualMachine]
                    $softwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration(
                        $machineReadinessDataForSoftwareUpdateConfiguration.ResourceId,
                        $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired,
                        $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentSuccessful,
                        $true,
                        $machineReadinessDataForSoftwareUpdateConfiguration.ErrorCode,
                        $machineReadinessDataForSoftwareUpdateConfiguration.ErrorMessage)
                }
                else 
                {
                    $softwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration($azureVirtualMachine, $false, $false, $true, [String]::Empty, [String]::Empty)
                }
            }    
        }
    
        $machinesToEnablePatchSettings = [System.Collections.ArrayList]@($softwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration.Keys)
        Enable-PatchSettingsForMachines($machinesToEnablePatchSettings)
    
        # Sync the Latest Errors from the Global Machines List
        foreach ($resourceId in $softwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration.Keys)
        {
            $machineReadinessDataForSoftwareUpdateConfiguration = $softwareUpdateConfigurationMigrationData.MachinesReadinessDataForSoftwareUpdateConfiguration[$resourceId]
            $softwareUpdateConfigurationMigrationData.UpdateMachineStatusForSoftwareUpdateConfiguration(
                $machineReadinessDataForSoftwareUpdateConfiguration.ResourceId,
                $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentRequired,
                $machineReadinessDataForSoftwareUpdateConfiguration.IsConfigAssignmentSuccessful,
                $machineReadinessDataForSoftwareUpdateConfiguration.IsMachineResolvedThroughAzureDynamicQuery,
                $Global:Machines[$resourceId].ErrorCode,
                $Global:Machines[$resourceId].ErrorMessage)
        }
    
        Write-Telemetry -Message ("Scheduled patching properties enabled for {0} machines in the software update configuration {1} " -f $countOfMachinesEnabledForScheduledPatching, $softwareUpdateConfiguration.id ) 
    
        # Finished Getting Machines Ready for Onboarding to Azure Update Manager.
        return Create-MaintenanceConfigurationFromSoftwareUpdateConfiguration -SoftwareUpdateConfigurationMigrationData $softwareUpdateConfigurationMigrationData -SoftwareUpdateConfiguration $softwareUpdateConfiguration -AutomationAccountResourceId $AutomationAccountResourceId            
    }
    catch [Exception]
    {
        Write-Telemetry -Message ("Failed to process software update configuration {0} with unhandled exception {1}." -f $SoftwareUpdateConfigurationId, $_.Exception.Message) -Level $ErrorLvl
        $softwareUpdateConfigurationMigrationData.MigrationStatus = $NotMigrated
        [void]$softwareUpdateConfigurationMigrationData.ErrorMessage.Add($_.Exception.Message)
        return $softwareUpdateConfigurationMigrationData
    }
}

function Initialize-JobSchedules
{
   <#
        .SYNOPSIS
            Gets schedules associated with Update Management master runbook and maintains a global list.
    
        .DESCRIPTION
            This command will get & maintain a global list of UM schedules with support for pagination.

        .PARAMETER AutomationAccountResourceId
            Automation Account Resource Id.

        .EXAMPLE
            Initialize-JobSchedules -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId
    )
    $output = $null
    $skip = 0
    do
    {
        $path = ($JobSchedulesWithPatchRunbookFilterPath -f $AutomationAccountResourceId, $skip)
        $output = Invoke-ArmApi-WithPath -Path $path -ApiVersion $AutomationAccountApiVersion -Method $GET
        if($output.Status -eq $Failed)
        {
            Write-Telemetry -Message ("Failed to get schedules with error code {0} and error message {1}." -f $output.ErrorCode, $output.ErrorMessage)
            throw
        }
        foreach ($result in $output.Response.value)
        {
            $properties = $result.properties
            if ($properties.runbook.name -eq $MasterRunbookName)
            {
                $parts = $properties.schedule.name.Split("_")
                $sucName = $parts[0 .. ($parts.Length - 2)] -join "_"
                if (!$Global:JobSchedules.ContainsKey($sucName))
                {
                    $Global:JobSchedules[$sucName] = [System.Collections.Generic.HashSet[String]]@()
                }
    
                [void]$Global:JobSchedules[$sucName].Add($properties.schedule.name)
            }
        }
        # API paginates in multiples of 100.
        $skip = $skip + 100
    }
    while ($null -ne $output.Response.nextLink);
}

function Disable-SoftwareUpdateConfiguration
{
   <#
        .SYNOPSIS
            Disables schedule associated with the Software Update Configuration.
    
        .DESCRIPTION
            This command will disable schedule associated with SUC.

        .PARAMETER AutomationAccountResourceId
            Automation Account Id.
        
        .PARAMETER ScheduleName
            Schedule name.
        
        .PARAMETER SoftwareUpdateConfigurationMigrationData
            Software update configuration migration data.

        .EXAMPLE
            Disable-SoftwareUpdateConfiguration -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}" -ScheduleName "PatchTuesday_xxxx" -SoftwareUpdateConfigurationMigrationData $SoftwareUpdateConfigurationMigrationData
    #>
    [CmdletBinding()]
    Param
    (
        [Parameter(Mandatory = $true, Position = 1)]
        [String]$AutomationAccountResourceId,

        [Parameter(Mandatory = $true, Position = 2)]
        [String]$ScheduleName,
        
        [Parameter(Mandatory = $true, Position = 3)]
        [SoftwareUpdateConfigurationMigrationData]$SoftwareUpdateConfigurationMigrationData
    )

    try
    {
        $path = ($AutomationSchedulesPath -f $AutomationAccountResourceId, $ScheduleName)
        $softwareUpdateConfigurationName = $ScheduleName.Split("_")
        $response = Invoke-ArmApi-WithPath -Path $path -ApiVersion $AutomationAccountApiVersion -Method $PATCH -Payload $DisableAutomationSchedule

        if ($response.Status -eq $Failed)
        {
            Write-Telemetry -Message ("Failed to Disable schedule {0} for software update configuration {1}." -f $ScheduleName, $softwareUpdateConfigurationName[0]) -Level $ErrorLvl
            $SoftwareUpdateConfigurationMigrationData.UnderlyingScheduleDisabled = $false
            return
        }

        Write-Telemetry -Message ("Disabled schedule {0} for software update configuration {1} as it is migrated successfully." -f $ScheduleName, $softwareUpdateConfigurationName[0])
        $SoftwareUpdateConfigurationMigrationData.UnderlyingScheduleDisabled = $true
    }
    catch [Exception]
    {
        $exceptionMessage = $_.Exception.Message
        $SoftwareUpdateConfigurationMigrationData.UnderlyingScheduleDisabled = $false
        Write-Telemetry -Message ("Failed to Disable schedule {0} for software update configuration {1} with exception {2}." -f $ScheduleName, $softwareUpdateConfigurationName[0], $exceptionMessage) -Level $ErrorLvl
    }
}

# Avoid clogging streams with Import-Modules outputs.
$VerbosePreference = "SilentlyContinue"

$azConnect = Connect-AzAccount -Identity -AccountId $UserManagedServiceIdentityClientId -SubscriptionId (Parse-ArmId -ResourceId $AutomationAccountResourceId).Subscription
if ($null -eq $azConnect)
{
    Write-Telemetry -Message ("Failed to connect with user managed identity. Please ensure that the user managed idenity is added to the automation account and having the required role assignments.") -Level $ErrorLvl
    throw
}
else
{
    Write-Telemetry -Message ("Successfully connected with account {0} to subscription {1}" -f $azConnect.Context.Account, $azConnect.Context.Subscription)
}

Populate-AllMachinesOnboardedToUpdateManagement -AutomationAccountResourceId $AutomationAccountResourceId

if ($EnablePeriodicAssessmentForMachinesOnboardedToUpdateManagement)
{
    Enable-PeriodicAssessmentOnAllMachines -AutomationAccountResourceId $AutomationAccountResourceId
}

if ($MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines)
{
    if ([String]::IsNullOrEmpty($ResourceGroupNameForMaintenanceConfigurations))
    {
        Write-Telemetry -Message ("Resource group name for maintenance configurations can't be null or empty") -Level $ErrorLvl
        throw
    }
    if ($ResourceGroupNameForMaintenanceConfigurations.Length -gt 36)
    {
        Write-Telemetry -Message ("Length of resource group name for maintenance configurations can't be more than 36 characters") -Level $ErrorLvl
        throw
    }

    Migrate-AllSoftwareUpdateConfigurationsToMaintenanceConfigurations -AutomationAccountResourceId $AutomationAccountResourceId -ResourceGroupNameForMaintenanceConfigurations $ResourceGroupNameForMaintenanceConfigurations
}
