#!/bin/bash

GIT_REPO=${PWD##*/}
echo S3_BUCKET $S3_BUCKET
echo GIT_REPO $GIT_REPO
echo LAMBDA_FUNCTION $LAMBDA_FUNCTION
function_dir=lambda/${LAMBDA_FUNCTION//-/_}
remote_dir=lambda/$LAMBDA_FUNCTION

echo checking for source code files in lambda function directory $function_dir ...
if [ -d $function_dir ]; then
  endpoint=s3://$S3_BUCKET/$remote_dir/
  echo uploading lambda source code to S3 at $endpoint ...
  aws s3 cp $function_dir $endpoint --recursive
else
  echo $function_dir directory not found in $GIT_REPO git repository.
fi

