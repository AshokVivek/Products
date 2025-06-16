.PHONY: build run logs
.SILENT: build run logs

LATEST_COMMIT_HASH = $(shell git rev-parse HEAD)
AWS_ACCESS_KEY_ID = $(shell aws configure get aws_access_key_id --profile ${aws_profile})
AWS_SECRET_ACCESS_KEY = $(shell aws configure get aws_secret_access_key --profile ${aws_profile})
AWS_SESSION_TOKEN = $(shell aws configure get aws_session_token --profile ${aws_profile})

compose_dev:
	LATEST_COMMIT_HASH=${LATEST_COMMIT_HASH} AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} docker compose -f compose.dev.yml up --build -d
