#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <tenant-id> <subscription-id>" >&2
  exit 1
fi

tenant_id="$1"
subscription_id="$2"
identity_py="/usr/lib/az/lib/python3.12/site-packages/azure/cli/core/auth/identity.py"

if ! grep -q 'port=8400 if self._is_adfs else None' "${identity_py}"; then
  echo "Azure CLI browser callback implementation has changed." >&2
  exit 1
fi

sed -i \
  's/port=8400 if self._is_adfs else None/port=8400/' \
  "${identity_py}"

browser_launcher="$(mktemp)"
trap 'rm -f "${browser_launcher}"' EXIT
cat >"${browser_launcher}" <<'EOF'
#!/usr/bin/env bash
echo
echo "Open this URL in a browser on the host:"
echo "$1"
echo
EOF
chmod +x "${browser_launcher}"

BROWSER="${browser_launcher}" az login \
  --tenant "${tenant_id}" \
  --output none
az account set --subscription "${subscription_id}"
az account show \
  --query '{subscription:name,subscriptionId:id,tenantId:tenantId,user:user.name}' \
  --output json
