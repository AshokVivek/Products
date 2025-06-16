.PHONY: build run logs
.SILENT: build run logs

# lowercases the stage and region parameters passed to the make command
lowercase_stage = $(shell echo ${stage} | tr A-Z a-z)
uppercase_stage = $(shell echo ${stage} | tr a-z A-Z)
lowercase_region = $(shell echo ${region} | tr A-Z a-z)
LATEST_COMMIT_HASH = $(shell git rev-parse HEAD)

# serverless confifuration file
serverless_configuration_filename = serverless_${lowercase_stage}.yml

# overriding serverless_configuration_filename if stage = prod and region is Indonesia
ifeq ($(lowercase_stage),prod)
    ifeq ($(lowercase_region),ap-southeast-3)
        serverless_configuration_filename := serverless_${lowercase_stage}_indonesia.yml
    endif
endif

# variables defining the target logging configuration for the selected lambdas
target_log_format = $(shell echo "JSON")
target_application_log_level = $(shell echo "INFO")
target_system_log_level = $(shell echo "WARN")
target_log_group = bank-connect-unified-logs-${lowercase_stage}

lambda_list = \
	bank-connect-${lowercase_stage}-cc_identity_with_ocr \
	bank-connect-${lowercase_stage}-identity_enhanced \
	bank-connect-${lowercase_stage}-identity_finvu_aa_enhanced \
	bank-connect-${lowercase_stage}-analyze_pdf_enhanced \
	bank-connect-${lowercase_stage}-analyze_pdf_page_enhanced \
	bank-connect-${lowercase_stage}-analyze_pdf_page_secondary_enhanced \
	bank-connect-${lowercase_stage}-analyze_pdf_page_ternary_enhanced \
	bank-connect-${lowercase_stage}-extract_transactions \
	bank-connect-${lowercase_stage}-extract_transactions_page \
	bank-connect-${lowercase_stage}-analyze_pdf_page_karur_enhanced \
	bank-connect-${lowercase_stage}-cc_transactions_with_ocr \
	bank-connect-${lowercase_stage}-cc_transactions_page_with_ocr \
	bank-connect-${lowercase_stage}-invoke_session_status_handler_enhanced \
	bank-connect-${lowercase_stage}-update_state_fan_out_enhanced \
	bank-connect-${lowercase_stage}-cache_subscribed_data \
	bank-connect-${lowercase_stage}-xml_report_handler \
	bank-connect-${lowercase_stage}-dms_push_handler

# creating this because make treats spaces differently for parsing parameters containing spaces
whitespace_delimiter = $(shell echo "000000")

# TODO: The following parameters to be made compatible with finboxdashboard parameters 
# mentioned in terraform/aws/scripts/up_django_apis_server.sh
# and Newly created SSM parameters to be removed later on:

# 1. BANK_CONNECT_CURRENT_STAGE_${uppercase_stage}
# 2. BANK_CONNECT_REGION_${uppercase_stage}
# 3. BANK_CONNECT_ENABLE_SENTRY_${uppercase_stage}
# 4. BANK_CONNECT_SENTRY_DSN_${uppercase_stage}
# 5. BANK_CONNECT_NANONETS_API_KEY_${uppercase_stage}
# 6. BANK_CONNECT_NANONETS_MODEL_ID_${uppercase_stage}
# 7. BANK_CONNECT_KAFKA_BROKER_URL_${uppercase_stage}
# 8. BANK_CONNECT_EXTRACTION_ISSUE_SLACK_TOKEN_${uppercase_stage}
# 9. BANK_CONNECT_EXTRACTION_ISSUE_SLACK_CHANNEL_${uppercase_stage}
# 10. BANK_CONNECT_CLOUDWATCH_LOG_GROUP_${uppercase_stage}

SSM_PARAMETER_NAMES := \
	BANK_CONNECT_CURRENT_STAGE_${uppercase_stage} \
	BANK_CONNECT_REGION_${uppercase_stage} \
	BANK_CONNECT_AWS_ACCOUNT_ID_${uppercase_stage} \
	BANK_CONNECT_IS_SERVER_${uppercase_stage} \
	BANK_CONNECT_INTERNAL_API_KEY_${uppercase_stage} \
	BANK_CONNECT_DJANGO_BASE_URL_${uppercase_stage} \
	BANK_CONNECT_ENABLE_SENTRY_${uppercase_stage} \
	BANK_CONNECT_SENTRY_DSN_${uppercase_stage} \
	BANK_CONNECT_INTERNAL_QUALITY_CHECK_URL_${uppercase_stage} \
	BANK_CONNECT_RECURRING_MICROSERVICE_TOKEN_${uppercase_stage} \
	BANK_CONNECT_QUALITY_SECRET_${uppercase_stage} \
	BANK_CONNECT_QUALITY_PRIVATE_IP_${uppercase_stage} \
	BANK_CONNECT_CATEGORIZE_RS_PRIVATE_IP_${uppercase_stage} \
	BANK_CONNECT_NANONETS_API_KEY_${uppercase_stage} \
	BANK_CONNECT_NANONETS_MODEL_ID_${uppercase_stage} \
	BANK_CONNECT_FSMLIB_CC_TEMPLATE_LOGGING_TASKS_QUEUE_URL_${uppercase_stage} \
	BANK_CONNECT_KAFKA_BROKER_URL_${uppercase_stage} \
	BANK_CONNECT_KAFKA_TOPIC_INCONSISTENCY_${uppercase_stage} \
	BANK_CONNECT_KAFKA_TOPIC_QUALITY_EVENTS_${uppercase_stage} \
	BANK_CONNECT_TCAP_DMS_ENDPOINT_${uppercase_stage} \
	BANK_CONNECT_TCAP_DMS_AUTH_KEY_${uppercase_stage} \
	BANK_CONNECT_PIPELINE_${uppercase_stage} \
	BANK_CONNECT_EXTRACTION_ISSUE_SLACK_TOKEN_${uppercase_stage} \
	BANK_CONNECT_EXTRACTION_ISSUE_SLACK_CHANNEL_${uppercase_stage} \
	BANK_CONNECT_PRIMARY_EXTRACTION_QUEUE_URL_${uppercase_stage} \
	BANK_CONNECT_SECONDARY_EXTRACTION_QUEUE_URL_${uppercase_stage} \
	BANK_CONNECT_CLOUDWATCH_LOG_GROUP_${uppercase_stage} \
	BANK_CONNECT_RECURRING_MICROSERVICE_URL_${uppercase_stage} \

SERVERLESS_PARAMETER_NAMES := \
	CURRENT_STAGE \
	REGION \
	AWS_ACCOUNT_ID \
	IS_SERVER \
	INTERNAL_API_KEY \
	DJANGO_BASE_URL \
	ENABLE_SENTRY \
	SENTRY_DSN \
	INTERNAL_QUALITY_CHECK_URL \
	RECURRING_MICROSERVICE_TOKEN \
	BANK_CONNECT_QUALITY_SECRET \
	BANK_CONNECT_QUALITY_PRIVATE_IP \
	CATEGORIZE_RS_PRIVATE_IP \
	NANONETS_API_KEY \
	NANONETS_MODEL_ID \
	FSMLIB_CC_TEMPLATE_LOGGING_TASKS_QUEUE_URL \
	KAFKA_BROKER_URL \
	KAFKA_TOPIC_INCONSISTENCY \
	KAFKA_TOPIC_QUALITY_EVENTS \
	TCAP_DMS_ENDPOINT \
	TCAP_DMS_AUTH_KEY \
	PIPELINE \
	EXTRACTION_ISSUE_SLACK_TOKEN \
	EXTRACTION_ISSUE_SLACK_CHANNEL \
	PRIMARY_EXTRACTION_QUEUE_URL \
	SECONDARY_EXTRACTION_QUEUE_URL \
	CLOUDWATCH_LOG_GROUP \
	RECURRING_MICROSERVICE_URL \

# Documentation for the aws update-function-configuration command used for updating lambda logging configuration:
# https://awscli.amazonaws.com/v2/documentation/api/latest/reference/lambda/update-function-configuration.html

# Corresponding create function:
# https://awscli.amazonaws.com/v2/documentation/api/latest/reference/lambda/create-function.html

deploy_and_update_lambda:

	@echo "######## Printing the value of makefile and command variables ########"
	@echo "Stage: $(lowercase_stage)"
	@echo "Region: $(lowercase_region)"
	@echo "Serverless configuration filename: $(serverless_configuration_filename)"
	@echo "Target log format: $(target_log_format)"
	@echo "Target application log level: $(target_application_log_level)"
	@echo "Target system log level: $(target_system_log_level)"
	@echo "Target log group: $(target_log_group)"
	@echo "Lambda list: $(lambda_list)"
	
	@echo "#### Invoking child target fetch_ssm_parameters_via_python and passing parameters to the serverless deploy command ####"
	$(eval SSM_PARAMETERS := $(shell $(MAKE) fetch_ssm_parameters_via_python stage=$(lowercase_stage) region=$(lowercase_region)))
	
	@echo "#### Extracted parameters: $(SSM_PARAMETERS) ####"

	@echo "################# Triggering Serverless Deploy #######################"
	serverless deploy \
		--aws-profile ${aws_profile} \
		$(SSM_PARAMETERS) \
		--config ${serverless_configuration_filename} \
		--verbose
	
	@echo "# Looping over lambda list and triggering AWS Lambda update function configuration function #"
	for lambda_name in ${lambda_list} \
	; do \
		aws lambda update-function-configuration \
			--profile ${aws_profile} \
			--region ${lowercase_region} \
			--function-name $$lambda_name \
			--no-cli-pager \
			--logging-config LogFormat=${target_log_format},ApplicationLogLevel=${target_application_log_level},SystemLogLevel=${target_system_log_level},LogGroup=${target_log_group} ; \
	done

deploy_and_update_lambda_server:

	@echo "######## Printing the value of makefile and command variables ########"
	@echo "Stage: $(lowercase_stage)"
	@echo "Region: $(lowercase_region)"
	@echo "Serverless configuration filename: $(serverless_configuration_filename)"
	@echo "Target log format: $(target_log_format)"
	@echo "Target application log level: $(target_application_log_level)"
	@echo "Target system log level: $(target_system_log_level)"
	@echo "Target log group: $(target_log_group)"
	@echo "Lambda list: $(lambda_list)"
	
	@echo "#### Invoking child target fetch_ssm_parameters_via_python and passing parameters to the serverless deploy command ####"
	$(eval SSM_PARAMETERS := $(shell $(MAKE) fetch_ssm_parameters_via_python stage=$(lowercase_stage) region=$(lowercase_region)))
	
	@echo "#### Extracted parameters: $(SSM_PARAMETERS) ####"

	@echo "################# Triggering Serverless Deploy #######################"
	serverless deploy \
		$(SSM_PARAMETERS) \
		--config ${serverless_configuration_filename} \
		--verbose
	
	@echo "# Looping over lambda list and triggering AWS Lambda update function configuration function #"
	for lambda_name in ${lambda_list} \
	; do \
		aws lambda update-function-configuration \
			--region ${lowercase_region} \
			--function-name $$lambda_name \
			--no-cli-pager \
			--logging-config LogFormat=${target_log_format},ApplicationLogLevel=${target_application_log_level},SystemLogLevel=${target_system_log_level},LogGroup=${target_log_group} ; \
	done

# test target created to individually test:
# 1. triggering the child fetch_ssm_parameters_via_python target
# 2. fetch the parameters
# 3. pass them onto serverless deploy
test_fetch_ssm_and_deploy:
	
	@echo "#### Invoking child target fetch_ssm_parameters_via_python and passing parameters to the serverless deploy command ####"
	$(eval SSM_PARAMETERS := $(shell $(MAKE) fetch_ssm_parameters_via_python stage=$(lowercase_stage) region=$(lowercase_region)))
	
	@echo "#### Extracted parameters: $(SSM_PARAMETERS) ####"

	@echo "################# Triggering Serverless Deploy #######################"
	serverless deploy \
		$(SSM_PARAMETERS) \
		--config ${serverless_configuration_filename} \
		--verbose

# unused target: kept for reference in case aws get-parameters to be triggered via cli. not being used because aws get-parameters just considers 10 keys at a time
# created to fetch the SSM parameter key- value pairs in a format directly consumable by the serverless command achieved via Python:
# --param=key=value
fetch_ssm_parameters:
	@aws ssm get-parameters \
		--region ${lowercase_region} \
		--names $(SSM_PARAMETER_NAMES) \
		--with-decryption \
		--query "Parameters[*].{Name:Name,Value:Value}" \
		--output json | \
		python3 -c "import sys, json; params = json.load(sys.stdin); print('\n'.join(f'--param=\"{p['Name']}={p['Value']}\"' for p in params))"

deploy_k8s_subscribers_dev:
	@echo "#################Deploying k8s image dev#################"
	aws ecr get-login-password --region ap-south-1 --profile ${aws_profile} | docker login --username AWS --password-stdin 909798297030.dkr.ecr.ap-south-1.amazonaws.com
	docker build -t bank-connect-consumers-dev -f subscriber.k8s.Dockerfile .
	docker tag bank-connect-consumers-dev:latest 909798297030.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-dev:${LATEST_COMMIT_HASH}
	docker push 909798297030.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-dev:${LATEST_COMMIT_HASH}


deploy_k8s_subscribers_uat:
	@echo "#################Deploying k8s image uat#################"
	aws ecr get-login-password --region ap-south-1 --profile ${aws_profile} | docker login --username AWS --password-stdin 951122091923.dkr.ecr.ap-south-1.amazonaws.com
	docker build -t bank-connect-consumers-uat -f subscriber.k8s.Dockerfile .
	docker tag bank-connect-consumers-uat:latest 951122091923.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-uat:${LATEST_COMMIT_HASH}
	docker push 951122091923.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-uat:${LATEST_COMMIT_HASH}


deploy_k8s_subscribers_prod:
	@echo "#################Deploying k8s image prod#################"
	aws ecr get-login-password --region ap-south-1 --profile ${aws_profile} | docker login --username AWS --password-stdin 905031918257.dkr.ecr.ap-south-1.amazonaws.com
	docker build -t bank-connect-consumers-prod -f subscriber.k8s.Dockerfile .
	docker tag bank-connect-consumers-prod:latest 905031918257.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-prod:${LATEST_COMMIT_HASH}
	docker push 905031918257.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-prod:${LATEST_COMMIT_HASH}

ECR_TAG ?= ${LATEST_COMMIT_HASH}
deploy_k8s_subscribers_dev_server:
	@echo "#################Deploying k8s image dev#################"
	aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 909798297030.dkr.ecr.ap-south-1.amazonaws.com; \
	docker build -t bank-connect-consumers-dev -f subscriber.k8s.Dockerfile .; \
	docker tag bank-connect-consumers-dev:latest 909798297030.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-dev:${ECR_TAG}; \
	docker push 909798297030.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-dev:${ECR_TAG}

deploy_k8s_subscribers_prod_server:
	@echo "#################Deploying k8s image prod#################"
	aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 905031918257.dkr.ecr.ap-south-1.amazonaws.com; \
	docker build -t bank-connect-consumers-prod -f subscriber.k8s.Dockerfile .; \
	docker tag bank-connect-consumers-prod:latest 905031918257.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-prod:${LATEST_COMMIT_HASH}; \
	docker push 905031918257.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-prod:${LATEST_COMMIT_HASH}

deploy_k8s_subscribers_uat_server:
	@echo "#################Deploying k8s image uat#################"
	aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 951122091923.dkr.ecr.ap-south-1.amazonaws.com; \
	docker build -t bank-connect-consumers-uat -f subscriber.k8s.Dockerfile .; \
	docker tag bank-connect-consumers-uat:latest 951122091923.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-uat:${LATEST_COMMIT_HASH}; \
	docker push 951122091923.dkr.ecr.ap-south-1.amazonaws.com/bank-connect-consumers-uat:${LATEST_COMMIT_HASH}

deploy_k8s_dev:
	@echo "#################Deploying k8s dev#################"
	chmod +x ./scripts/create_k8s_deployment_pr.sh
	./scripts/create_k8s_deployment_pr.sh dev local ${aws_profile}

deploy_k8s_uat:
	@echo "#################Deploying k8s uat#################"
	chmod +x ./scripts/create_k8s_deployment_pr.sh
	./scripts/create_k8s_deployment_pr.sh uat local ${aws_profile}

# this target performs the following steps:
# 1. invokes a python script which: batches the parameters and gets their values in the format: --param="key1=value1" --param="key2=value2"
# 2. converts --param=key1=value1 --param=key2=value2 INTO --param="key1=value1" --param="key2=value2"
# 3. echoes the value to be consumed by the parent targets
# 4. usage of whitespace delimiter: pattern substitution does not work properly with spaces, so initializing and passing a delimiter to the python script to 
# 	 replace all whitespaces with the delimiter, and then replacing that delimiter back with a space in make so that double quote encapsulation works properly
FETCH_SSM_PARAMETER_PYTHON_SCRIPT = python3 fetch_ssm_parameters.py
fetch_ssm_parameters_via_python:

	$(eval SSM_PARAMETERS := $(shell $(FETCH_SSM_PARAMETER_PYTHON_SCRIPT) $(lowercase_stage) $(lowercase_region) $(whitespace_delimiter) $(words $(SSM_PARAMETER_NAMES)) $(SSM_PARAMETER_NAMES) $(SERVERLESS_PARAMETER_NAMES)))
	
	$(eval SSM_PARAMETERS_WITH_QUOTES := $(patsubst --param=%, --param=\"%\", $(SSM_PARAMETERS)))
	$(eval SSM_PARAMETERS_WITH_QUOTES := $(subst $(whitespace_delimiter), , $(SSM_PARAMETERS_WITH_QUOTES)))

	@echo $(SSM_PARAMETERS_WITH_QUOTES)
