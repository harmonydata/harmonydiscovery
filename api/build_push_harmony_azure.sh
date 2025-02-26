az login
az acr login --name twharmonyacr
az acr update -n twharmonyacr --admin-enabled true
export COMMIT_ID=`git show -s --format=%ci_%h | sed s/[^_a-z0-9]//g | sed s/0[012]00_/_/g` && docker build -t harmonydiscoveryapiweaviate -t harmonydiscoveryapiweaviate:$COMMIT_ID -t twharmonyacr.azurecr.io/harmonydiscoveryapiweaviate -t twharmonyacr.azurecr.io/harmonydiscoveryapiweaviate:$COMMIT_ID --build-arg COMMIT_ID=$COMMIT_ID . && docker push twharmonyacr.azurecr.io/harmonydiscoveryapiweaviate:$COMMIT_ID && echo "The container version is $COMMIT_ID"
