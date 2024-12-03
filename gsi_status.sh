#!/bin/bash

## Trip ctrl-c
trap 'echo ""; echo "Encountered ctrl + c. Exiting!"; exit 1' SIGINT

## Get region for aws cli --region
EC2_AVAIL_ZONE=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`
EC2_REGION=$(sed 's/[a-z]$//' <<< "${EC2_AVAIL_ZONE}")

echo "Checking GSI index status.."
echo "Creating.."
startts=`date +%s`
while true; do
    STATUS=$(aws dynamodb describe-table --region="${EC2_REGION}" --table-name fleet --query 'Table.GlobalSecondaryIndexes[].IndexStatus' --output text)
    ([[ ${STATUS} != 'ACTIVE' ]] && sleep 5 ) || break
    clear
    echo "Checking GSI index status.."
    echo "Creating.."
    echo "elapsed (seconds): $((`date +%s` - startts))" 
done
echo "Index status: ${STATUS}. Proceed to next step."