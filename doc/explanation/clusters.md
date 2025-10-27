---
discourse: lxc:[Scriptlet&#32;based&#32;instance&#32;placement&#32;scheduler](15728)
---

(exp-clusters)=
# Clusters

```{youtube} https://www.youtube.com/watch?v=nrOR6yaO_MY
```

To spread the total workload over several servers, LXD can be run in clustering mode.
In this scenario, any number of LXD servers share the same distributed database that holds the configuration for the cluster members and their instances.
The LXD cluster can be managed uniformly using the [`lxc`](lxc.md) client or the REST API.

This feature was introduced as part of the [`clustering`](../api-extensions.md#clustering) API extension and is available since LXD 3.0.

```{tip}
If you want to quickly set up a basic LXD cluster, check out [MicroCloud](https://canonical.com/microcloud).
```

(clustering-members)=
## Cluster members

A LXD cluster consists of one bootstrap server and at least two further cluster members.
It stores its state in a [distributed database](../database.md), which is a [Dqlite](https://canonical.com/dqlite) database replicated using the Raft algorithm.

While you could create a cluster with only two members, it is strongly recommended that the number of cluster members be at least three.
With this setup, the cluster can survive the loss of at least one member and still be able to establish quorum for its distributed state.

When you create the cluster, the Dqlite database runs on only the bootstrap server until a third member joins the cluster.
Then both the second and the third server receive a replica of the database.

See {ref}`cluster-form` for more information.

(clustering-member-roles)=
### Member roles

In a cluster with three members, all members replicate the distributed database that stores the state of the cluster.
If the cluster has more members, only some of them replicate the database.
The remaining members have access to the database, but don't replicate it.

At each time, there is an elected cluster leader that monitors the health of the other members.

Each member that replicates the database has either the role of a *voter* or of a *stand-by*.
If the cluster leader goes offline, one of the voters is elected as the new leader.
If a voter member goes offline, a stand-by member is automatically promoted to voter.
The database (and hence the cluster) remains available as long as a majority of voters is online.

The following roles can be assigned to LXD cluster members.
Automatic roles are assigned by LXD itself and cannot be modified by the user.

| Role                  | Automatic     | Description |
| :---                  | :--------     | :---------- |
| `database`            | yes           | Voting member of the distributed database |
| `database-leader`     | yes           | Current leader of the distributed database |
| `database-standby`    | yes           | Stand-by (non-voting) member of the distributed database |
| `event-hub`           | no            | Exchange point (hub) for the internal LXD events (requires at least two) |
| `ovn-chassis`         | no            | Uplink gateway candidate for OVN networks |

The default number of voter members ({config:option}`server-cluster:cluster.max_voters`) is three.
The default number of stand-by members ({config:option}`server-cluster:cluster.max_standby`) is two.
With this configuration, your cluster will remain operational as long as you switch off at most one voting member at a time.

See {ref}`cluster-manage` for more information.

(clustering-offline-members)=
#### Offline members and fault tolerance

If a cluster member is down for more than the configured offline threshold, its status is marked as offline.
In this case, no operations are possible on this member, and neither are operations that require a state change across all members.

As soon as the offline member comes back online, operations are available again.

If the member that goes offline is the leader itself, the other members will elect a new leader.

If you can't or don't want to bring the server back online, you can [delete it from the cluster](cluster-manage-delete-members).

You can tweak the amount of seconds after which a non-responding member is considered offline by setting the {config:option}`server-cluster:cluster.offline_threshold` configuration.
The default value is 20 seconds.
The minimum value is 10 seconds.

To automatically {ref}`evacuate <cluster-evacuate>` instances from an offline member, set the {config:option}`server-cluster:cluster.healing_threshold` configuration to a non-zero value.

See {ref}`cluster-recover` for more information.

#### Failure domains

You can use failure domains to indicate which cluster members should be given preference when assigning roles to a cluster member that has gone offline.
For example, if a cluster member that currently has the database role gets shut down, LXD tries to assign its database role to another cluster member in the same failure domain, if one is available.

To update the failure domain of a cluster member, use the [`lxc cluster edit <member>`](lxc_cluster_edit.md) command and change the `failure_domain` property from `default` to another string.

(clustering-member-config)=
### Member configuration

LXD cluster members are generally assumed to be identical systems.
This means that all LXD servers joining a cluster must have an identical configuration to the bootstrap server, in terms of storage pools and networks.

To accommodate things like slightly different disk ordering or network interface naming, there is an exception for some configuration options related to storage and networks, which are member-specific.

When such settings are present in a cluster, any server that is being added must provide a value for them.
Most often, this is done through the interactive `lxd init` command, which asks the user for the value for a number of configuration keys related to storage or networks.

Those settings typically include:

- The source device and size (quota) for a storage pool
- The name for a ZFS zpool, LVM thin pool or LVM volume group
- External interfaces and BGP next-hop for a bridged network
- The name of the parent network device for managed `physical` or `macvlan` networks

See {ref}`cluster-config-storage` and {ref}`cluster-config-networks` for more information.

If you want to look up the questions ahead of time (which can be useful for scripting), query the `/1.0/cluster` API endpoint.
This can be done through `lxc query /1.0/cluster` or through other API clients.

## Images

By default, LXD replicates images on as many cluster members as there are database members.
This typically means up to three copies within the cluster.

You can increase that number to improve fault tolerance and the likelihood of the image being locally available.
To do so, set the {config:option}`server-cluster:cluster.images_minimal_replica` configuration.
The special value of `-1` can be used to have the image copied to all cluster members.

(cluster-groups)=
## Cluster groups

In a LXD cluster, you can add members to cluster groups.
You can use these cluster groups to launch instances on a cluster member that belongs to a subset of all available members.
For example, you could create a cluster group for all members that have a GPU and then launch all instances that require a GPU on this cluster group.

By default, all cluster members belong to the `default` group.

See {ref}`howto-cluster-groups` and {ref}`cluster-target-instance` for more information.

(clustering-instance-placement)=
## Automatic placement of instances

In a cluster setup, each instance lives on one of the cluster members.
When you launch an instance, you can target it to a specific cluster member, to a cluster group or have LXD automatically assign it to a cluster member.

By default, the automatic assignment picks the cluster member that has the lowest number of instances.
If several members have the same amount of instances, one of the members is chosen at random.

However, you can control this behavior with the {config:option}`cluster-cluster:scheduler.instance` configuration option:

- If `scheduler.instance` is set to `all` for a cluster member, this cluster member is selected for an instance if:

   - The instance is created without `--target` and the cluster member has the lowest number of instances.
   - The instance is targeted to live on this cluster member.
   - The instance is targeted to live on a member of a cluster group that the cluster member is a part of, and the cluster member has the lowest number of instances compared to the other members of the cluster group.

- If `scheduler.instance` is set to `manual` for a cluster member, this cluster member is selected for an instance if:

   - The instance is targeted to live on this cluster member.

- If `scheduler.instance` is set to `group` for a cluster member, this cluster member is selected for an instance if:

   - The instance is targeted to live on this cluster member.
   - The instance is targeted to live on a member of a cluster group that the cluster member is a part of, and the cluster member has the lowest number of instances compared to the other members of the cluster group.

(exp-clusters-placement)=
### Placement groups

Placement groups provide declarative control over how instances are distributed across cluster members.
They define both a **policy** (how instances should be distributed) and a **rigor** (how strictly the policy is enforced).

Placement groups are project-scoped resources, which means different projects can have placement groups with the same name without conflict.

See {ref}`cluster-placement-groups` for usage instructions and {ref}`ref-placement-groups` for reference documentation.

(clusters-high-availability)=
## High availability

Clusters provide two types of high availability (HA):

- Control plane HA (ensuring that clients can always access the cluster)
- Data plane HA (ensuring that workloads continue to run)

(clusters-high-availability-control)=
### High availability of the control plane (client access)

Each cluster member can {ref}`expose an API endpoint <server-expose>` through its {config:option}`server-core:core.https_address`. Through this access point, a remote client can communicate with any cluster member in multiple ways:

- Through the API (see {ref}`authentication` and {ref}`rest-api`)
- Through the {ref}`LXD web UI client <access-ui>`
- By setting up {ref}`remote servers <remotes>` for CLI access

Because the cluster database is distributed, access to any member gives you access to the entire control plane. If one server goes down, you can still manage the cluster through the other members. This provides the basis for control plane HA.

The limitation is that on the client side, you must either manually switch to another member's access point if your chosen server is unavailable, or implement your own client-side logic to cycle through a list of access points.

For a single, highly available access point to the control plane, you can add on a routing service that configures a virtual IP. See our how-to guide: {ref}`howto-cluster-vip`.

(clusters-high-availability-data)=
### High availability of the data plane (workloads)

LXD clusters enable HA of workloads (instances) in multiple ways:

Cluster evacuation
: Instances can be manually evacuated from one cluster member to another, providing planned high availability during maintenance. This includes live migration for virtual machines. See: {ref}`cluster-evacuate`.

Cluster healing
: If a cluster member fails and {config:option}`server-cluster:cluster.healing_threshold` is set, it automatically restarts instances on that member on a healthy member of the cluster. See: {ref}`cluster-healing`.

Virtual networking
: On clusters using {ref}`OVN networking <network-ovn>`, logical switches/routers are distributed across the cluster. This means that instance NICs remain reachable even if the server hosting one OVN chassis goes offline.

Storage redundancy
: On clusters using Ceph for storage, if a disk or cluster member fails, the data is still available elsewhere in the Ceph cluster.

Shared storage
: Volumes using the {ref}`Ceph RBD <storage-ceph>` and {ref}`CephFS <storage-cephfs>` storage drivers are accessible from all cluster members. If the member hosting an instance fails, its volumes can be reattached to another member.

## Related topics

{{clustering_how}}

{{clustering_ref}}
