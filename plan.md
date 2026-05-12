Nice.

Now we will create several new branches for each API group:
- Storage Pools:
- Storage Buckets: Buckets + Keys
- Networks: Networks
- Network Integrations: ACLs + Forwards + Load Balancers + Peers
- Network Zones: Zones + Records

For example, Network Zone Records branch becomes: feat/async-network-zone-records

In each branch, we need to further separate commits.

For example, changes in Network zone records are:

Network Zone Records:
    POST /1.0/network-zones/{zone}/records
    PUT /1.0/network-zones/{zone}/records/{name}
    DELETE /1.0/network-zones/{zone}/records/{name}

This guarantees us at least 3 commits (Create/Post, Update/Put, Delete) in endpoints, 3 commits in client, and 3 commits in lxc.

Retain API extension commit across all branches the same. When the first one gets merged, I will rebase others and this commit will be dropped (they all need to land under same API extension).
