# Moving from many small to one large automerge document

Single backend with multiple frontends. Anna KV inspired.

Differently to Anna KV we will have one backend per server so only one amount
of syncing so the cluster will seem less dense. We have little use for
consistent hashing on the keyspace as transactions can be across keys.

## Frontends

On an etcd request the tokio task should begin to handle the request (checking
it is ok etc.) and then needs to acquire a thread local frontend. This should
then make the change to the frontend, get the resulting uncompressedchange and
the request can return from here. Asynchronously the uncompressedchange should
be put on a channel to the single backend in order to be saved (this may be
synchronous if we want persistence). When the backend has patches these will be
sent on a channel back to the frontend to be applied.

## Backend

The backend receives changes on a channel, applies them as local changes and
returns the patch. This may also lead to updates needing to be propagated to
other local frontends so it should send these. Changes should also be streamed
to neighbours as they appear and syncing should take place regularly.

For persistence the backend should first save the change in sled using the
actor id and sequence number as keys and then acknowledge the change. After
this it can perform the application to the backend itself.

Periodically compaction of the persisted changes should take place, getting the
changes in the backend, saving the backend and then writing this value. Once
this value is written we can remove the previously extracted changes from sled.

## Todo

- [x] investigate thread locals use with tokio tasks (one frontend but multiple clients trying to use it)
  - done using actors, need to create correct number of actors first and then balance across them but just using one for now
- [x] look into persisting changes in sled (https://github.com/automerge/automerge/issues/331)
  - done in automerge-persistent
- [ ] Internal and external replication factor
  - backend parameters that ensure differing consistency
  - internal for ensuring how many frontends on the local node apply the patch before we return
  - external for how many other nodes apply our changes before we return
