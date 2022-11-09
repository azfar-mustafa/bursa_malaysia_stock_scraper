#!/bin/bash

# To initialize credentials in variables
. ./.env

# Login into azure account
az account set -s $subscription

# Create resource group
if [ $(az group exists --name $resourceGroup) = false ]; then
    az group create --name $resourceGroup --location "$location"
    echo "Resource group $resourceGroup is created"
else echo "Resource group $resourceGroup is existed"
fi


# Create general purpose storage account
#az storage account create --resource-group $resourceGroup --name $storageAccount --sku Standard_LRS

#echo "Storage account $storageAccount is created"



# Create function app in azure
az functionapp create --resource-group $resourceGroup --consumption-plan-location "$functionLocation" --runtime python --runtime-version 3.8 --functions-version 3 --name $appName --os-type linux --storage-account $storageAccount