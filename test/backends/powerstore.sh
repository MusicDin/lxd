powerstore_setup() {
  local LXD_DIR="${1}"

  echo "==> Setting up PowerStore backend in ${1}"
}

# powerstore_configure creates PowerStore storage pool and configures instance root disk
# device in default profile to use that storage pool.
powerstore_configure() {
  local LXD_DIR="${1}"
  local POOL_NAME="${2:-"lxdtest-${LXD_DIR##*/}"}" # Use the last part of the LXD_DIR as pool name
  local VOLUME_SIZE="${3:-"${DEFAULT_VOLUME_SIZE}"}"

  echo "==> Configuring PowerStore backend in ${LXD_DIR}"

  # Create powerstore storage pool.
  lxc storage create "${POOL_NAME}" powerstore \
    powerstore.gateway="${POWERSTORE_GATEWAY}" \
    powerstore.gateway.verify="${POWERSTORE_GATEWAY_VERIFY:-true}" \
    powerstore.user.name="${POWERSTORE_USER:-admin}" \
    powerstore.user.password="${POWERSTORE_PASSWORD}" \
    powerstore.mode="${POWERSTORE_MODE:-nvme/tcp}" \
    volume.size="${VOLUME_SIZE}"

  # Add the storage pool to the default profile.
  lxc profile device add default root disk path="/" pool="${POOL_NAME}"
}

# configure_powerstore_pool creates new PowerStore storage pool with a given name.
# Additional arguments are appended to the lxc storage create command.
# If there is anything on the stdin, the content is passed to the lxc storage create command as stdin as well.
configure_powerstore_pool() {
  poolName="${1}"
  shift 1

  if [ -p /dev/stdin ]; then
    # Use heredoc if there's input on stdin
    lxc storage create "${poolName}" powerstore \
      powerstore.gateway="${POWERSTORE_GATEWAY}" \
      powerstore.gateway.verify="${POWERSTORE_GATEWAY_VERIFY:-true}" \
      powerstore.user.name="${POWERSTORE_USER:-admin}" \
      powerstore.user.password="${POWERSTORE_PASSWORD}" \
      powerstore.mode="${POWERSTORE_MODE:-nvme/tcp}" \
      "$@" <<EOF
$(cat)
EOF
  else
    # Run without stdin if no heredoc is provided
    lxc storage create "${poolName}" powerstore \
      powerstore.gateway="${POWERSTORE_GATEWAY}" \
      powerstore.gateway.verify="${POWERSTORE_GATEWAY_VERIFY:-true}" \
      powerstore.user.name="${POWERSTORE_USER:-admin}" \
      powerstore.user.password="${POWERSTORE_PASSWORD}" \
      powerstore.mode="${POWERSTORE_MODE:-nvme/tcp}" \
      "$@"
  fi
}

powerstore_teardown() {
  local LXD_DIR="${1}"

  echo "==> Tearing down PowerStore backend in ${LXD_DIR}"
}
