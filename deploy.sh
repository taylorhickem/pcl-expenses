#!/bin/bash

files_compare () {
  endpoint=s3://$S3_BUCKET/git/$GIT_REPO/$2
  aws s3 cp $endpoint $remote_dir/$3
  if [ $? == 0 ]; then
    if [ "$(cat $2)" != "$(cat $remote_dir/$3)" ]; then
      ACTION_TYPES=$ACTION_TYPES,$1
    fi
  else
    echo $3 not found at $endpoint
      ACTION_TYPES=$ACTION_TYPES,$1
  fi
}

remote_dir=remote
GIT_REPO=${PWD##*/}
ACTION_TYPES='"function_update"'

echo checking for changes to classify deploy actions ...
echo GIT_REPO $GIT_REPO
echo LAYERS_PATH $LAYERS_PATH
echo CFN_TEMPLATE_PATH $CFN_TEMPLATE_PATH

mkdir remote_files

echo checking for changes in layers.json ...
files_compare '"layers_update"' $LAYERS_PATH layers.json

echo checking for changes in cfn_template.yaml ...
files_compare '"stack_update"' $CFN_TEMPLATE_PATH cfn_template.yaml

if [ $? == 0 ]; then
  rm -r remote_files
  ACTION_TYPES=[$ACTION_TYPES]
  echo ACTION_TYPES $ACTION_TYPES
  invoke_payload='{"ACTION_TYPES":'$ACTION_TYPES',"FUNCTION_NAME":"'$LAMBDA_FUNCTION'"}'
  echo invoking deployment lambda $DEPLOY_LAMBDA with payload $invoke_payload ...
  aws lambda invoke \
    --function-name $DEPLOY_LAMBDA \
    --payload $invoke_payload \
    --cli-binary-format raw-in-base64-out \
    /dev/stdout
fi