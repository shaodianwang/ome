# charts/

## Deployment commands

```bash
make install              # Deploy to cluster via kustomize (interactive confirmation)
make deploy-helm          # Deploy via Helm charts
make run-ome-manager      # Run controller locally against configured cluster
make delete-webhooks      # Remove webhooks (needed for local controller debugging)
```
