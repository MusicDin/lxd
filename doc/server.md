(server)=
# Server configuration

The LXD server can be configured through a set of key/value configuration options.

The key/value configuration is namespaced.
The following options are available:

- {ref}`server-options-core`
- {ref}`server-options-acme`
- {ref}`server-options-oidc`
- {ref}`server-options-cluster`
- {ref}`server-options-images`
- {ref}`server-options-loki`
- {ref}`server-options-misc`

See {ref}`server-configure` for instructions on how to set the configuration options.

```{note}
Options marked with a `global` scope are immediately applied to all cluster members.
Options with a `local` scope must be set on a per-member basis.
```

(server-options-core)=
## Core configuration

The following server options control the core daemon configuration:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-core start -->
    :end-before: <!-- config group server-core end -->
```

(server-options-acme)=
## ACME configuration

The following server options control the {ref}`ACME <authentication-server-certificate>` configuration:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-acme start -->
    :end-before: <!-- config group server-acme end -->
```

(server-options-oidc)=
## OpenID Connect configuration

The following server options configure external user authentication through {ref}`authentication-openid`:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-oidc start -->
    :end-before: <!-- config group server-oidc end -->
```

```{important}
Setting `oidc.client.secret` may prevent CLI clients from authenticating depending on the Identity Provider policies.
Set this key only if required by the Identity Provider.
```

(server-options-cluster)=
## Cluster configuration

The following server options control {ref}`clustering`:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-cluster start -->
    :end-before: <!-- config group server-cluster end -->
```

(server-options-images)=
## Images configuration

The following server options configure how to handle {ref}`images`:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-images start -->
    :end-before: <!-- config group server-images end -->
```

(server-options-loki)=
## Loki configuration

The following server options configure the external log aggregation system:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-loki start -->
    :end-before: <!-- config group server-loki end -->
```

(server-options-misc)=
## Miscellaneous options

The following server options configure server-specific settings for {ref}`instances`, MAAS integration, {ref}`OVN <network-ovn>` integration, {ref}`Backups <backups>` and {ref}`storage`:

% Include content from [metadata.txt](metadata.txt)
```{include} metadata.txt
    :start-after: <!-- config group server-miscellaneous start -->
    :end-before: <!-- config group server-miscellaneous end -->
```

## Related topics

How-to guides:

- {ref}`server-configure`
