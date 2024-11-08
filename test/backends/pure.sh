pure_setup() {
  local LXD_DIR

  LXD_DIR=$1

  echo "==> Setting up PureStorage backend in ${LXD_DIR}"
}

pure_configure() {
  local LXD_DIR

  LXD_DIR=$1

  echo "==> Configuring PureStorage backend in ${LXD_DIR}"

  # Create pure storage storage pool.
  lxc storage create "lxdtest-$(basename "${LXD_DIR}")" pure \
    pure.gateway="${PURESTORAGE_GATEWAY}" \
    pure.gateway.verify="${PURESTORAGE_GATEWAY_VERIFY:-true}" \
    pure.api.token="${PURESTORAGE_API_TOKEN}" \
    pure.iscsi.address="${PURESTORAGE_ISCSI_ADDRESS}" \
    pure.mode="${PURESTORAGE_MODE:-iscsi}" \
    volume.size=25MiB

  # Add the storage pool to the default profile.
  lxc profile device add default root disk path="/" pool="lxdtest-$(basename "${LXD_DIR}")"
}

pure_teardown() {
  local LXD_DIR

  LXD_DIR=$1

  echo "==> Tearing down PureStorage backend in ${LXD_DIR}"
}
