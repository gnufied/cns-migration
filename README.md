# Migrate CNS volumes between datastores

## How to use it:

```
./bin/cns-migration --source samsung-nvme --destination nfs-shared -volume-file ./pv.txt
```


Where `pv.txt` contains a list of persistent volumes which are to be migrated.

If a volume in question is in-use by a pod, the tool will not migrate the volume and skip it. Similarly, if a specified volume
is already migrated and can't be found in source datastore, then also the volume will be simply skipped.
