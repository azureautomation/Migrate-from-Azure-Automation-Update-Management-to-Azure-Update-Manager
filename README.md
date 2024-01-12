# Migrate-from-Azure-Automation-Update-Management-to-Azure-Update-Manager
This Powershell script is designed to migrate machines and schedules onboarded to the legacy Azure Automation Update Management solution to the latest Azure Update Manager.

### DESCRIPTION
1. This runbook will do one of the below based on the parameters provided to it.
2. The runbook will be using the User-Assigned Managed Identity whose client id is passed as a parameter for authenticating ARM calls. Please ensure the managed idenity is assigned the proper role definitions before executing this runbook.
3. Non-Azure machines which are not onboarded to Arc will not be onboarded to Azure Update Manager.
	1. If EnablePeriodicAssessmentForMachinesOnboardedToUpdateManagement parameter is true, it will enable periodic assessment for all azure/arc machines onboarded to Automation Update Management under the automation account where the runbook is executing.
    2. If MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines parameters is true,
    	1. It will enable periodic asssessment for all azure/arc machines either attached or picked up through azure dynamic queries of software update configurations under the automation account where the runbook is executing.
        2. It will set required patch properties for scheduled patching for all azure machines either attached or picked up through dynamic queries of software update configurations under the automation account where the runbook is executing.
        3. It will migrate software update configurations by creating equivalent MRP maintenance configurations. Maintenance configurations will be created in the region where the automation account resides and in the resource group provided as input.
        	1. Pre/Post tasks of software update configurations will not be migrated.
            2. Saved search queries of software update configurations will not be migrated.

### PARAMETER AutomationAccountResourceId
        Mandatory. Automation Account Resource Id.

### PARAMETER UserManagedServiceIdentityClientId
        Mandatory. Client Id of the User Assigned Managed Idenitity.

### PARAMETER EnablePeriodicAssessmentForMachinesOnboardedToUpdateManagement
        Optional.

### PARAMETER MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines
        Optional.

### PARAMETER ResourceGroupNameForMaintenanceConfigurations
        Optional. The resource group name should not be more than 36 characters.
        The resource group name which will be used for creating a resource group in the same region as the automation account. The maintenance configurations for the migrated software update configurations from the automation account will be residing here.

### EXAMPLE
        Migration -AutomationAccountResourceId "/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Automation/automationAccounts/{aaName}"  -ClientId "########-####-####-####-############" -EnablePeriodicAssessment $true MigrateSchedulesAndEnablePeriodicAssessmentForLinkedMachines $true

### OUTPUTS
        Outputs the status of machines and software update configurations post migration to the output stream.