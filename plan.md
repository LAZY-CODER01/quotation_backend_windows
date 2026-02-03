<!-- TAG=$(date +%s)
docker build -t quotesnapacr.azurecr.io/quotesnap-backend:$TAG .
docker push quotesnapacr.azurecr.io/quotesnap-backend:$TAG

az webapp config container set \
  --name quotesnap-api \
  --resource-group quotesnap-rg \
  --container-image-name quotesnapacr.azurecr.io/quotesnap-backend:$TAG

az webapp restart \
  --name quotesnap-api \
  --resource-group quotesnap-rg -->
