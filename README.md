## create k8 secret
```cmd
kubectl create secret generic ondc-open-data-webapp-secret --from-literal=db_password=********
```

```cmd
kubectl create secret generic ondc-open-data-webapp-aws-secret --from-literal=AWS_ACCESS_KEY=********* -n ondc-open-data
```

```cmd
kubectl create secret generic ondc-open-data-webapp-aws-secret-key --from-literal=AWS_SECRET_KEY=****** -n ondc-open-data
```
