# run

`docker compose up -d` 

# in openshift 

* run `oc apply -f k8s` in root dir 
* `oc expose svc/api`
* `oc get route api`