#!/bin/bash

set -e

####################################################################################
############################# Utility Functions ####################################
####################################################################################

log_info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

log_warning() {
    echo -e "\033[1;33m[WARNING]\033[0m $1"
}

log_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
    return 1
}

github_check_repo_access() {
    local owner=$1
    local repo=$2
    local token=$3

    log_info "Checking if repository is accessible..."
    
    local response
    response=$(curl -s -H "Authorization: Bearer ${token}" \
        "https://api.github.com/repos/${owner}/${repo}")
    
    if echo "$response" | grep -q "\"name\": \"$repo\""; then
        log_success "Repository found. Proceeding..."
        return 0
    else
        log_error "Repository not found or unauthorized access."
        return 1
    fi
}

github_get_branch_sha() {
    local owner=$1
    local repo=$2
    local branch=$3
    local token=$4
    
    local response
    response=$(curl -s -H "Authorization: Bearer ${token}" \
        "https://api.github.com/repos/${owner}/${repo}/branches/${branch}")

    if ! echo "$response" | grep -q "\"name\": \"$branch\""; then
        log_error "Unable to get SHA for branch: ${branch}, check if branch name is correct!" >&2
        return 1
    fi

    local commit_sha
    commit_sha=$(echo "$response" | grep -o '"sha": "[^"]*"' | head -1 | cut -d'"' -f4)

    if [ -z "${commit_sha}" ]; then
        log_error "Failed to get latest commit SHA." >&2
        return 1
    fi

    printf "%s\n" "$commit_sha"
    return 0
}

github_create_branch() {
    local owner=$1
    local repo=$2
    local new_branch=$3
    local base_sha=$4
    local token=$5

    log_info "Creating new branch: ${new_branch} from SHA: ${base_sha}"
    
    local response
    response=$(curl -s -X POST "https://api.github.com/repos/${owner}/${repo}/git/refs" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -d "{\"ref\": \"refs/heads/${new_branch}\", \"sha\": \"${base_sha}\"}")

    if echo "${response}" | grep -q "Reference already exists"; then
        log_error "Branch already exists. This should not happen. Try again!"
        return 1
    elif ! echo "${response}" | grep -q "ref"; then
        log_error "Branch creation failed."
        return 1
    fi
    
    log_success "Branch created successfully"
    return 0
}

github_get_directory_contents() {
    local owner=$1
    local repo=$2
    local path=$3
    local branch=$4
    local token=$5
    
    local response
    response=$(curl -s -H "Authorization: Bearer ${token}" \
        "https://api.github.com/repos/${owner}/${repo}/contents/${path}?ref=${branch}")

    if echo "$response" | grep -q '"message":'; then
        local error_msg
        if command -v jq &>/dev/null; then
            error_msg=$(echo "$response" | jq -r '.message')
        else
            error_msg=$(echo "$response" | grep -o '"message": "[^"]*"' | cut -d'"' -f4)
        fi
        log_error "Failed to get directory contents: ${error_msg}" >&2
        return 1
    fi

    printf "%s\n" "$response"
    return 0
}

github_get_directory_contents_recursive() {
    local owner=$1
    local repo=$2
    local path=$3
    local branch=$4
    local token=$5

    local response
    response=$(github_get_directory_contents "$owner" "$repo" "$path" "$branch" "$token")
    
    if [ $? -ne 0 ]; then
        return 1
    fi

    local all_item_paths=""

    while read -r item; do
        local type=$(echo "$item" | jq -r '.type')
        local item_path=$(echo "$item" | jq -r '.path')

        if [ "$type" = "dir" ]; then
    
            local nested_items
            nested_items=$(github_get_directory_contents_recursive "$owner" "$repo" "$item_path" "$branch" "$token")
            if [ $? -eq 0 ]; then
                all_item_paths="${all_item_paths}${nested_items}"
            fi
        elif [ "$type" = "file" ]; then
    
            all_item_paths="${all_item_paths}${item_path}:"
        fi
    done < <(echo "$response" | jq -c '.[]')

    if [ -n "$all_item_paths" ]; then
        printf "%s" "$all_item_paths"
    fi
    return 0
}

github_update_file() {
    local owner=$1
    local repo=$2
    local file_path=$3
    local content=$4
    local message=$5
    local file_sha=$6
    local branch=$7
    local token=$8

    log_info "Updating file: ${file_path}"
    
    local encoded_content

    if [ -z "${content}" ] || [ ${#content} -lt 10 ]; then
        log_error "Content for ${file_path} is empty or suspiciously short (${#content} bytes). Aborting to prevent data loss."
        return 1
    fi
    
    encoded_content=$(printf '%s' "${content}" | base64 | tr -d '\n')

    if [ -z "${encoded_content}" ]; then
        log_error "Base64 encoding failed for ${file_path}. Aborting to prevent data loss."
        return 1
    fi
    
    local json_payload="{\"message\":\"${message}\",\"content\":\"${encoded_content}\",\"sha\":\"${file_sha}\",\"branch\":\"${branch}\"}"
    
    local response
    response=$(curl -s -X PUT \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${token}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "https://api.github.com/repos/${owner}/${repo}/contents/${file_path}" \
        -d "${json_payload}")
    
    if echo "${response}" | grep -q "content"; then
        return 0
    else
        log_error "Failed to update ${file_path}"
        log_error "API Response: ${response}"
        return 1
    fi
}

github_create_pull_request() {
    local owner=$1
    local repo=$2
    local title=$3
    local head=$4
    local base=$5
    local token=$6
    
    local response
    response=$(curl -s -X POST "https://api.github.com/repos/${owner}/${repo}/pulls" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -d "{\"title\": \"${title}\", \"head\": \"${head}\", \"base\": \"${base}\"}")
    
    if echo "${response}" | grep -q "html_url"; then
        local pr_url
        pr_url=$(echo "${response}" | jq -r '.html_url')
        echo $pr_url
        return 0
    else
        log_error "Failed to create pull request"
        log_error "API Response: ${response}"
        return 1
    fi
}

github_merge_pull_request() {
    local owner=$1
    local repo=$2
    local pull_number=$3
    local token=$4
    local merge_method=${5:-"merge"}
    local commit_message=${7:-""}
    
    log_info "Attempting to merge pull request #${pull_number}..."
    
    local payload="{\"merge_method\": \"${merge_method}\", \"bypass_rules\": true"
    
    if [ -n "$commit_message" ]; then
        payload="${payload}, \"commit_message\": \"${commit_message}\""
    fi
    
    payload="${payload}}"
    
    local response
    response=$(curl -s -X PUT "https://api.github.com/repos/${owner}/${repo}/pulls/${pull_number}/merge" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -H "Accept: application/vnd.github.v3+json" \
        -d "$payload")
        
    if echo "${response}" | grep -q "successfully merged"; then
        log_success "Pull request #${pull_number} successfully merged"
        return 0
    else
        local error_msg
        if command -v jq &>/dev/null; then
            error_msg=$(echo "$response" | jq -r '.message // "Unknown error"')
        else
            error_msg=$(echo "$response" | grep -o '"message": "[^"]*"' | cut -d'"' -f4)
        fi
        log_error "Failed to merge pull request: ${error_msg}"
        return 1
    fi
}

github_delete_branch() {
    local owner=$1
    local repo=$2
    local branch_name=$3
    local token=$4
    
    log_info "Attempting to delete branch '${branch_name}'..."
    
    local response
    response=$(curl -s -X DELETE "https://api.github.com/repos/${owner}/${repo}/git/refs/heads/${branch_name}" \
        -H "Authorization: Bearer ${token}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -H "Accept: application/vnd.github.v3+json")
    
    # Check if the response is empty (successful deletion returns 204 No Content)
    if [ -z "$response" ]; then
        log_success "Branch '${branch_name}' successfully deleted"
        return 0
    else
        local error_msg
        if command -v jq &>/dev/null; then
            error_msg=$(echo "$response" | jq -r '.message // "Unknown error"')
        else
            error_msg=$(echo "$response" | grep -o '"message": "[^"]*"' | cut -d'"' -f4)
        fi
        log_error "Failed to delete branch: ${error_msg}"
        return 1
    fi
}

get_pr_number_from_url() {
    local pr_url=$1
    
    local pr_number
    pr_number=$(echo "$pr_url" | grep -o '/pull/[0-9]\+' | cut -d'/' -f3)
    
    if [ -z "$pr_number" ]; then
        log_error "Failed to extract PR number from URL: $pr_url"
        return 1
    fi
    
    echo "$pr_number"
    return 0
}

fetch_github_pat_from_secrets() {
  local secret_name="$1"
  
  if [ -z "$AWS_PROFILE" ] || [ "$AWS_PROFILE" == "default" ]; then    
    secret_json=$(aws secretsmanager get-secret-value --region ap-south-1 --secret-id "$secret_name" --query SecretString --output text)
  else    
    secret_json=$(aws secretsmanager get-secret-value --region ap-south-1 --profile "$AWS_PROFILE" --secret-id "$secret_name" --query SecretString --output text)
  fi

  # Extract token value using jq
  token=$(echo "$secret_json" | jq -r '.token')

  if [ -n "$token" ]; then
    echo "$token"
  else
    log_error "Failed to fetch token from secret."
    return 1
  fi
}

####################################################################################
################################## Main Script #####################################
####################################################################################

ENV="${1:-dev}"
RUNNER="${2:-local}"
AWS_PROFILE="${3}"


log_info "Starting deployment to environment: $ENV"

####################################################################################
################################## Configuration ###################################
####################################################################################

INFRA_REPO_OWNER="finbox-in"
INFRA_REPO_NAME="bank-connect-infra"
INFRA_BASE_BRANCH="develop"
AWS_ACCOUNT_ID=""
NEW_BRANCH_NAME="feature/BANC-0000-${ENV}-image-update-$(date +%s)"
MANIFEST_DIR="k8s/extraction/environments/finbox/in/${ENV}"
ECR_IMAGE_NAME="bank-connect-consumers-${ENV}"

case "$ENV" in
    "prod") AWS_ACCOUNT_ID="905031918257" ;; 
    "dev") AWS_ACCOUNT_ID="909798297030" ;;
    "uat") AWS_ACCOUNT_ID="951122091923" ;;
    *) log_error "Invalid environment: $ENV" && exit 1 ;;
esac

ECR_REPO_URL="${AWS_ACCOUNT_ID}.dkr.ecr.ap-south-1.amazonaws.com"
ECR_IMAGE_URL="${ECR_REPO_URL}/${ECR_IMAGE_NAME}"
GITHUB_TOKEN=""
COMMIT_MESSAGE=""
NEW_IMAGE_HASH=""
GITHUB_SECRET_PAT_NAME="github_admin_token_bank_connect_infra"

log_info "Repository: ${INFRA_REPO_OWNER}/${INFRA_REPO_NAME}, base branch: ${INFRA_BASE_BRANCH}"

####################################################################################
################## Build new image and push to ECR #################################
####################################################################################

LATEST_COMMIT_HASH=$(git rev-parse HEAD)
NEW_IMAGE_HASH="${LATEST_COMMIT_HASH}"

if [ -z "$AWS_PROFILE" ] || [ "$AWS_PROFILE" == "default" ]; then
  log_info "Attempting ECR login without explicit profile..."
  aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin ${ECR_REPO_URL}
else
  log_info "Attempting ECR login with profile: ${AWS_PROFILE}..."
  aws ecr get-login-password --region ap-south-1 --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin ${ECR_REPO_URL}
fi

log_info "Building Docker image..."
docker build --provenance=false -t ${ECR_IMAGE_NAME} -f subscriber.k8s.Dockerfile .

log_info "Tagging Docker image..."
docker tag ${ECR_IMAGE_NAME}:latest ${ECR_IMAGE_URL}:${LATEST_COMMIT_HASH}

log_info "Pushing Docker image to ECR..."
docker push ${ECR_IMAGE_URL}:${LATEST_COMMIT_HASH}

log_success "Successfully pushed image: ${ECR_REPO_URL}/${ECR_IMAGE_NAME}:${NEW_IMAGE_HASH}"

####################################################################################
############################### Setup GitHub token #################################
####################################################################################

GITHUB_TOKEN=$(fetch_github_pat_from_secrets "$GITHUB_SECRET_PAT_NAME")

if [ -z "$GITHUB_TOKEN" ]; then
  log_error "Failed to fetch GitHub token from secrets."
  exit 1
fi

####################################################################################
############################ Get Commit Message ####################################
####################################################################################

echo "Runner: $RUNNER"

LAST_LOCAL_COMMIT=$(git log -1 --pretty=%B)
DEFAULT_MESSAGE="${LAST_LOCAL_COMMIT}"

if [ "$RUNNER" = "local" ]; then
  echo "Enter the commit message (${INFRA_REPO_NAME}) [default: ${DEFAULT_MESSAGE}]: "
  read COMMIT_MESSAGE
  
  COMMIT_MESSAGE_TRIMMED=$(echo "$COMMIT_MESSAGE" | tr -d '[:space:]')
  if [ -z "$COMMIT_MESSAGE_TRIMMED" ]; then
    COMMIT_MESSAGE="${DEFAULT_MESSAGE}"
    echo "Using default commit message: ${COMMIT_MESSAGE}"
  fi
else
  COMMIT_MESSAGE="${DEFAULT_MESSAGE}"
  echo "Non-local runner detected. Using last commit message: ${COMMIT_MESSAGE}"
fi

#if env is prod runner should be github aciton
if [ "$ENV" = "prod" ] && [ "$RUNNER" != "github_action" ]; then
  log_error "Invalid runner for prod environment. Use 'github' runner."
  exit 1
fi

COMMIT_MESSAGE=$(echo "$COMMIT_MESSAGE" | head -n 1)

####################################################################################
###################### GitHub Repository Operations ################################
####################################################################################

github_check_repo_access "$INFRA_REPO_OWNER" "$INFRA_REPO_NAME" "$GITHUB_TOKEN" || exit 1

LATEST_COMMIT_SHA=$(github_get_branch_sha "$INFRA_REPO_OWNER" "$INFRA_REPO_NAME" "$INFRA_BASE_BRANCH" "$GITHUB_TOKEN") || exit 1

github_create_branch "$INFRA_REPO_OWNER" "$INFRA_REPO_NAME" "$NEW_BRANCH_NAME" "$LATEST_COMMIT_SHA" "$GITHUB_TOKEN" || exit 1

##############################################################################
# Update repository files in the manifest directory with new image tags ######
##############################################################################

log_info "Processing files in ${MANIFEST_DIR} to update image tags..."

GIT_CONTENT_RESPONSE=$(github_get_directory_contents_recursive "$INFRA_REPO_OWNER" "$INFRA_REPO_NAME" "$MANIFEST_DIR" "$INFRA_BASE_BRANCH" "$GITHUB_TOKEN")

OLDIFS="$IFS"
IFS=$':' FILE_PATHS=($GIT_CONTENT_RESPONSE)
IFS="$OLDIFS"

for file_path in "${FILE_PATHS[@]}"; do

    [ -z "$file_path" ] && continue

    file_response=$(curl -s "https://api.github.com/repos/${INFRA_REPO_OWNER}/${INFRA_REPO_NAME}/contents/${file_path}?ref=${INFRA_BASE_BRANCH}" \
        -H "Authorization: Bearer ${GITHUB_TOKEN}")
    
    file_sha=$(echo "${file_response}" | jq -r '.sha')
    
    encoded_file_content=$(echo "${file_response}" | jq -r '.content')
    file_content=$(echo "${encoded_file_content}" | tr -d '\n' | base64 -d)
    
    updated_content=$(echo "${file_content}" | \
        sed "s|image: \"${ECR_IMAGE_URL}:[^\"]*\"|image: \"${ECR_IMAGE_URL}:${NEW_IMAGE_HASH}\"|g" | \
        sed "s|LATEST_COMMIT_HASH: \"[^\"]*\"|LATEST_COMMIT_HASH: \"${LATEST_COMMIT_HASH}\"|g" | \
        sed "s|tags.datadoghq.com/version: \"[^\"]*\"|tags.datadoghq.com/version: \"${LATEST_COMMIT_HASH}\"|g")

    if [ "${file_content}" = "${updated_content}" ]; then
        log_info "No changes needed for ${file_path}, skipping..."
        continue
    fi

    if [ -z "${updated_content}" ] || [ ${#updated_content} -lt 10 ]; then
        log_error "Updated content for ${file_path} is empty or suspiciously short. Aborting to prevent data loss."
        log_error "Original file length: ${#file_content}, Updated file length: ${#updated_content}"
        exit 1
    fi
    
    update_message="Update image tag to ${NEW_IMAGE_HASH} and Datadog version tag to ${LATEST_COMMIT_HASH}"
    github_update_file "$INFRA_REPO_OWNER" "$INFRA_REPO_NAME" "$file_path" "$updated_content" "$update_message" "$file_sha" "$NEW_BRANCH_NAME" "$GITHUB_TOKEN" || exit 1
done < <(printf '%s\n' "$GIT_CONTENT_RESPONSE")

##############################################################################
######################## Create and merge the pull request ###################
##############################################################################

COMMIT_MESSAGE="$ENV deployment - ${COMMIT_MESSAGE}"

pr_url=$(github_create_pull_request "$INFRA_REPO_OWNER" "$INFRA_REPO_NAME" "$COMMIT_MESSAGE" "$NEW_BRANCH_NAME" "$INFRA_BASE_BRANCH" "$GITHUB_TOKEN") || exit 1

echo "PR URL: ${pr_url}"

##############################################################################
######################## Merge the pull request #############################
##############################################################################

PR_NUMBER=$(get_pr_number_from_url "$pr_url")

if [ -n "$PR_NUMBER" ]; then
    MERGE_METHOD="squash"
    
    github_merge_pull_request "${INFRA_REPO_OWNER}" "${INFRA_REPO_NAME}" "${PR_NUMBER}" "${GITHUB_TOKEN}" "${MERGE_METHOD}" "${COMMIT_MESSAGE}" || {
        log_warning "Automatic merge failed. You may need to merge the PR manually after any required checks have passed."
        log_info "PR URL: ${pr_url}"
    }    
else
    log_error "Failed to extract PR number from URL: ${pr_url}"
    log_info "You'll need to merge this PR manually."
fi


##############################################################################
######################## (Cleanup) Delete the Branch #############################
##############################################################################

github_delete_branch "${INFRA_REPO_OWNER}" "${INFRA_REPO_NAME}" "${NEW_BRANCH_NAME}" "${GITHUB_TOKEN}"

log_success "$ENV: deployment process completed successfully!"