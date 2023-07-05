#!/bin/bash
#
# TO-DO: run the following command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 
#[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "AKIA4QE4NTH3R7EBEANN", 
# "password": "s73eJIJRbnqRtll0/YKxyVYgrDWXfoRpJCDkcG2m", 
# "port": null, 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@"
#}]

# [{"id": "67", 
# "conn_id": "aws_credentials", 
# "conn_type": "aws", 
# "description": null, 
# "host": "", 
# "schema": "", 
# "login": "AKIAUE55KS4Y32W7PFFX", 
# "password": "Oe2XHVpiTFyI+JhAKDX/VoGzgawqAcaT7/7E3vip", 
# "port": null, 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {}, 
# "get_uri": "aws://AKIAUE55KS4Y32W7PFFX:Oe2XHVpiTFyI%2BJhAKDX%2FVoGzgawqAcaT7%2F7E3vip@"}]
#
# Copy the value after "get_uri":
#
# For example: aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@
#
# TO-DO: Update the following command with the URI and un-comment it:
#
#airflow connections add aws_credentials --conn-uri 'aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@'

airflow connections add aws_credentials --conn-uri 'aws://AKIAUE55KS4Y32W7PFFX:Oe2XHVpiTFyI%2BJhAKDX%2FVoGzgawqAcaT7%2F7E3vip@'
#
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get redshift -o json
# 
# [{"id": "3", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default.859321506295.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "R3dsh1ft", 
# "port": "5439", 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
# [{"id": "68", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default.285475510065.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "abcABC123!", 
# "port": "5439", 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:abcABC123%21@default.285475510065.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# TO-DO: Update the following command with the URI and un-comment it:
#
#airflow connections add redshift --conn-uri 'redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
airflow connections add redshift --conn-uri 'redshift://awsuser:abcABC123%21@default.285475510065.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket udacity-dend
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines