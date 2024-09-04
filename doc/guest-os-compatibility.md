(guest-os-compatibility)=
# Guest OS compatibility

The following operating systems (OS) were tested as virtual machine guest running on top of on LXD `5.21/stable`. Each OS was tested by doing a manual installation using the official ISO as provided by the vendor.

OS vendor | OS version         | OS support | [LXD agent](#lxd-agent) | VirtIO-SCSI | VirtIO-BLK | NVMe    | CSM (BIOS) | UEFI | Secure Boot
:---      | :---               | :---       | :---                    | :---        | :---       | :---    | :---       | :--- | :---
CentOS    | CentOS 6.10 [^1]   | EOL        | ❌ [^2]                 | ✅          | ❌ [^7]    | 🟢      | ✅         | ❌   | ❌
CentOS    | CentOS 7.9         | EOL        | ❌ [^2]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
CentOS    | CentOS 8.5         | EOL        | 🟢 [^3]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
CentOS    | CentOS 8-Stream    | EOL        | 🟢 [^3]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
CentOS    | CentOS 9-Stream    | Supported  | 🟢 [^3]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Red Hat   | RHEL 7.9           | EOL        | ❌ [^2]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Red Hat   | RHEL 8.10          | Supported  | 🟢 [^3]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Red Hat   | RHEL 9.4           | Supported  | 🟢 [^3]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
SUSE      | SLES 12 SP5        | Supported  | ✅                      | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
SUSE      | SLES 15 SP6        | Supported  | ✅                      | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Ubuntu    | 14.04.6 LTS        | EOL        | ❌ [^8]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Ubuntu    | 16.04.7 LTS        | ESM        | ✅ [^9]                 | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Ubuntu    | 18.04.6 LTS        | ESM        | ✅                      | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Ubuntu    | 20.04.6 LTS        | Supported  | ✅                      | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Ubuntu    | 22.04.4 LTS        | Supported  | ✅                      | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Ubuntu    | 24.04.1 LTS        | Supported  | ✅                      | ✅          | 🟢         | 🟢      | 🟢         | ✅   | ✅
Windows   | Server 2012        | Supported  | ➖                      | ✅          | 🟢         | ❌      | 🟢         | ✅   | ✅
Windows   | Server 2016        | Supported  | ➖                      | ✅          | 🟢         | 🟢 [^4] | ❌ [^6]    | ✅   | ✅
Windows   | Server 2019        | Supported  | ➖                      | ✅          | 🟢         | 🟢      | ❌ [^6]    | ✅   | ✅
Windows   | Server 2022        | Supported  | ➖                      | ✅          | 🟢         | 🟢      | ❌ [^6]    | ✅   | ✅
Windows   | 10 22H2            | Supported  | ➖                      | ✅          | 🟢         | 🟢      | ❌ [^6]    | ✅   | ✅
Windows   | 11 23H2 [^5]       | Supported  | ➖                      | ✅          | 🟢         | 🟢      | ❌         | ✅   | ✅

[^1]: No network support despite having VirtIO-NET module.
[^2]: Support for 9P or `virtiofs` not available. Note: CentOS 7 has a `kernel-plus` kernel with 9P support allowing LXD agent to work (with `selinux=0`).
[^3]: Requires disabling SELinux to allow LXD agent loading `virtiofs` configuration share.
[^4]: NVMe disks are visible but the installer lists all 255 namespaces slowing down the initialization.
[^5]: A virtual TPM is required.
[^6]: The OS installer hangs when booting in CSM/BIOS mode.
[^7]: The OS installer hangs when booting with VirtIO-BLK despite having VirtIO-BLK supported by the kernel.
[^8]: This Linux version does not use `systemd` which the LXD agent requires.
[^9]: Requires the HWE kernel (`4.15`) for proper `vsock` support which is required by the LXD agent.

Legend         | Icon
:---           | :---
recommended    | ✅
supported      | 🟢
not applicable | ➖
not supported  | ❌

## Notes

### LXD agent

The LXD agent provides the ability to execute commands inside of the virtual machine guest without relying on traditional access solution like secure shell (SSH) or Remote Desktop Protocol (RDP). This agent is only supported on Linux guests using `systemd`.
See {ref}`instances-create-iso` for instructions on how to install the LXD agent manually.

### CSM/BIOS boot

```bash
lxc config set v1 security.secureboot=false
lxc config set v1 security.csm=true
```

### Virtual TPM

```bash
lxc config device add v1 vtpm tpm path=/dev/tpm0
```

### VirtIO-BLK or NVMe

```bash
lxc config device override v1 root io.bus=virtio-blk
# or
lxc config device override v1 root io.bus=nvme
```

### Disconnect the ISO

```bash
lxc config device remove v1 iso
```