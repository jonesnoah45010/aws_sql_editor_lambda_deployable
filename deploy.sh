#!/usr/bin/env bash
# Deploy Lambda (container image) + optional public Function URL, with ENV support.
# Works on macOS Bash 3.2 (no associative arrays). Flask app reads env via os.getenv("KEY").
set -euo pipefail

# ---------- Config (override via env) ----------
AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || echo us-east-2)}"
FUNCTION_NAME="${FUNCTION_NAME:-aws_db_app}"
ECR_REPO="${ECR_REPO:-$FUNCTION_NAME}"
ARCH="${ARCH:-x86_64}"          # x86_64 or arm64
MEMORY_MB="${MEMORY_MB:-512}"
TIMEOUT_SEC="${TIMEOUT_SEC:-15}"
ROLE_NAME="${ROLE_NAME:-LambdaBasicExecutionRole}"   # Used if ROLE_ARN not set
ROLE_ARN="${ROLE_ARN:-}"                              # If provided, skips role creation/lookup
DOCKERFILE="${DOCKERFILE:-Dockerfile}"               # Dockerfile for Lambda image
CREATE_URL="${CREATE_URL:-1}"                        # Set 0 to skip Function URL
ENV_FILE="${ENV_FILE:-}"                             # or pass --env-file .env
CLEAR_ENV=0                                          # or pass --clear-env

# ---------- CLI args ----------
ENV_PAIRS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)         ENV_PAIRS+=("$2"); shift 2 ;;
    --env-file)    ENV_FILE="$2"; shift 2 ;;
    --clear-env)   CLEAR_ENV=1; shift ;;
    -r|--region)   AWS_REGION="$2"; shift 2 ;;
    -n|--name)     FUNCTION_NAME="$2"; shift 2 ;;
    --repo)        ECR_REPO="$2"; shift 2 ;;
    --arch)        ARCH="$2"; shift 2 ;;
    --memory)      MEMORY_MB="$2"; shift 2 ;;
    --timeout)     TIMEOUT_SEC="$2"; shift 2 ;;
    --role-arn)    ROLE_ARN="$2"; shift 2 ;;
    --role-name)   ROLE_NAME="$2"; shift 2 ;;
    --dockerfile)  DOCKERFILE="$2"; shift 2 ;;
    --no-url)      CREATE_URL=0; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Send log lines to stderr so command-substitution only captures values.
log() { printf "\n\033[1m%s\033[0m\n" "$*" 1>&2; }

# ---------- Build ENV JSON for --environment (Bash 3.2 compatible) ----------
build_env_json() {
  set +u
  if [[ $CLEAR_ENV -eq 1 ]]; then
    printf '%s' '{"Variables":{}}'
    set -u; return
  fi

  KEYS=(); VALS=()
  kv_set() {
    local _k="$1" _v="$2" i
    if [[ "${_v}" == \"*\" && "${_v}" == *\" ]]; then _v="${_v%\"}"; _v="${_v#\"}"; fi
    if [[ "${_v}" == \'*\' && "${_v}" == *\' ]]; then _v="${_v%\'}"; _v="${_v#\'}"; fi
    for i in "${!KEYS[@]}"; do
      if [[ "${KEYS[$i]}" == "$_k" ]]; then VALS[$i]="${_v}"; return; fi
    done
    KEYS+=("$_k"); VALS+=("$_v")
  }

  if [[ -n "$ENV_FILE" ]]; then
    if [[ ! -f "$ENV_FILE" ]]; then echo "Env file not found: $ENV_FILE" >&2; set -u; exit 3; fi
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
      line="${line#export }"
      k="${line%%=*}"; v="${line#*=}"
      k="$(printf '%s' "$k" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"
      [[ -z "$k" ]] && continue
      kv_set "$k" "$v"
    done < "$ENV_FILE"
  fi

  for pair in "${ENV_PAIRS[@]}"; do
    k="${pair%%=*}"; v="${pair#*=}"
    kv_set "$k" "$v"
  done

  if [[ ${#KEYS[@]} -eq 0 ]]; then
    printf '%s' '{"Variables":{}}'
    set -u; return
  fi

  json='{"Variables":{'
  i=0
  while [[ $i -lt ${#KEYS[@]} ]]; do
    [[ $i -gt 0 ]] && json+=','
    k_esc="${KEYS[$i]//\\/\\\\}"; k_esc="${k_esc//\"/\\\"}"
    v_raw="${VALS[$i]}"; v_esc="${v_raw//\\/\\\\}"; v_esc="${v_esc//\"/\\\"}"
    json+="\"$k_esc\":\"$v_esc\""
    i=$((i+1))
  done
  json+='}}'
  printf '%s' "$json"
  set -u
}

# ---------- Wait helpers for Lambda state ----------
print_lambda_status() {
  local fn="$1" region="$2"
  aws lambda get-function-configuration \
    --function-name "$fn" --region "$region" \
    --query '{State:State,LastUpdateStatus:LastUpdateStatus,StateReason:StateReason,LastUpdateStatusReason:LastUpdateStatusReason}' \
    --output text 2>/dev/null || true
}

wait_for_lambda_ready() {
  local fn="$1" region="$2"
  # Try built-in waiter first
  if aws lambda wait function-updated --function-name "$fn" --region "$region" >/dev/null 2>&1; then
    return 0
  fi
  # Fallback manual polling
  local i state status
  for i in {1..40}; do
    state="$(aws lambda get-function-configuration --function-name "$fn" --region "$region" --query 'State' --output text 2>/dev/null || echo unknown)"
    status="$(aws lambda get-function-configuration --function-name "$fn" --region "$region" --query 'LastUpdateStatus' --output text 2>/dev/null || echo unknown)"
    if [[ "$state" != "Pending" && "$status" == "Successful" ]]; then
      return 0
    fi
    if [[ "$status" == "Failed" ]]; then
      echo "Lambda update FAILED:" >&2
      print_lambda_status "$fn" "$region" >&2
      return 1
    fi
    sleep 3
  done
  echo "Timed out waiting for Lambda to be ready. Last status:" >&2
  print_lambda_status "$fn" "$region" >&2
  return 1
}

# ---------- Ensure IAM Role ----------
ensure_role() {
  if [[ -n "$ROLE_ARN" ]]; then echo "$ROLE_ARN"; return; fi
  ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || true)"
  if [[ -z "$ARN" || "$ARN" == "None" ]]; then
    log "Creating IAM role $ROLE_NAME"
    TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "$TRUST" >/dev/null
    aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null
    sleep 8
    ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)"
  fi
  echo "$ARN"
}

# ---------- ECR helpers ----------
ensure_repo() {
  aws ecr describe-repositories --repository-names "$ECR_REPO" --region "$AWS_REGION" >/dev/null 2>&1 || \
    aws ecr create-repository --repository-name "$ECR_REPO" --region "$AWS_REGION" >/dev/null
}

wait_for_ecr_image() {
  local repo="$1" tag="$2" region="$3"
  for i in {1..12}; do
    if aws ecr describe-images --repository-name "$repo" --image-ids imageTag="$tag" --region "$region" >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  echo "ECR image $repo:$tag not visible yet in $region" >&2
  return 1
}

# ---------- Build & push image ----------
build_and_push() {
  log "Logging in to ECR"
  aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "$(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.$AWS_REGION.amazonaws.com"

  local ACCOUNT REPO_URI TAG IMG
  ACCOUNT="$(aws sts get-caller-identity --query 'Account' --output text)"
  REPO_URI="$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO"
  TAG="$(date +%Y%m%d-%H%M%S)"
  IMG="$REPO_URI:$TAG"

  log "Building image ($ARCH)"
  docker buildx build --platform "linux/$ARCH" -f "$DOCKERFILE" -t "$IMG" .

  log "Pushing image $IMG"
  docker push "$IMG" >/dev/stderr

  wait_for_ecr_image "$ECR_REPO" "$TAG" "$AWS_REGION"
  echo "$IMG"
}

# ---------- Create or update Lambda ----------
update_code_with_retry() {
  local fn="$1" region="$2" img="$3" i=0
  for i in {1..10}; do
    if aws lambda update-function-code \
          --function-name "$fn" --region "$region" --image-uri "$img" >/dev/null 2>&1; then
      return 0
    fi
    # If conflict, wait and try again
    sleep 4
  done
  # last try without redirect so user sees the error
  aws lambda update-function-code \
    --function-name "$fn" --region "$region" --image-uri "$img" >/dev/null
}

ensure_function_url_public() {
  # Make Function URL exist (AuthType NONE), and attach public permission with required condition.
  # No qualifier is used (simplest + avoids $LATEST.PUBLISHED gotchas).
  log "Ensuring Function URL (public, no-auth)"
  aws lambda create-function-url-config \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --auth-type NONE >/dev/null 2>&1 || \
  aws lambda update-function-url-config \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --auth-type NONE >/dev/null

  # Remove any prior statements (qualified/unqualified) then add the correct one.
  aws lambda remove-permission \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --statement-id FunctionUrlAllowPublicAccess >/dev/null 2>&1 || true

  aws lambda remove-permission \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --qualifier '$LATEST.PUBLISHED' \
    --statement-id FunctionUrlAllowPublicAccess >/dev/null 2>&1 || true

  aws lambda add-permission \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --statement-id FunctionUrlAllowPublicAccess \
    --action lambda:InvokeFunctionUrl \
    --principal "*" \
    --function-url-auth-type NONE >/dev/null

  URL="$(aws lambda get-function-url-config --function-name "$FUNCTION_NAME" --region "$AWS_REGION" \
        --query 'FunctionUrl' --output text 2>/dev/null || true)"
  if [[ -n "$URL" && "$URL" != "None" ]]; then
    echo
    echo "Function URL: $URL"
    echo "Try: curl -i \"$URL\""
  fi
}

upsert_lambda() {
  local IMG_URI="$1"
  local ROLE ENV_JSON
  ROLE="$(ensure_role)"
  ENV_JSON="$(build_env_json)"

  if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    log "Updating function configuration (memory/timeout/env)"
    aws lambda update-function-configuration \
      --function-name "$FUNCTION_NAME" \
      --region "$AWS_REGION" \
      --memory-size "$MEMORY_MB" \
      --timeout "$TIMEOUT_SEC" \
      --environment "$ENV_JSON" >/dev/null

    # Wait until config update is done before pushing code
    wait_for_lambda_ready "$FUNCTION_NAME" "$AWS_REGION"

    log "Updating function code (image: $IMG_URI)"
    update_code_with_retry "$FUNCTION_NAME" "$AWS_REGION" "$IMG_URI"

    # Wait again until code update finishes
    wait_for_lambda_ready "$FUNCTION_NAME" "$AWS_REGION"
  else
    log "Creating function (image: $IMG_URI)"
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --package-type Image \
      --code "ImageUri=$IMG_URI" \
      --role "$ROLE" \
      --architectures "$ARCH" \
      --memory-size "$MEMORY_MB" \
      --timeout "$TIMEOUT_SEC" \
      --region "$AWS_REGION" \
      --environment "$ENV_JSON" >/dev/null

    wait_for_lambda_ready "$FUNCTION_NAME" "$AWS_REGION"
  fi

  if [[ "$CREATE_URL" -eq 1 ]]; then
    ensure_function_url_public
  fi
}

# ---------- Main ----------
log "Ensuring ECR repository: $ECR_REPO"
ensure_repo

IMG="$(
  build_and_push | tail -n1
)"

if [[ -z "${IMG:-}" ]]; then
  echo "ERROR: No image URI captured from build step." >&2
  exit 1
fi

log "Image: $IMG"
upsert_lambda "$IMG"
log "Done."

# Usage examples:
#   ./deploy.sh --env-file .env
#   ./deploy.sh --env API_KEY=abc123 --env DEBUG=false
#   ./deploy.sh --clear-env
#   ./deploy.sh -n my-flask-fn -r us-east-2 --arch arm64 --memory 1024 --timeout 30
