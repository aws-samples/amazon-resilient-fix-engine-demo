aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <ECR BASE URL LIKE: 123456789123.dkr.ecr.us-east-1.amazonaws.com>

docker build -t jlt/fixengineonaws .

docker tag jlt/fixengineonaws:latest <ECR IMAGE URL LIKE: 123456789123.dkr.ecr.us-east-1.amazonaws.com/jlt/fixengineonaws:latest>

docker push <ECR IMAGE URL LIKE: 123456789123.dkr.ecr.us-east-1.amazonaws.com/jlt/fixengineonaws:latest>

aws ecs update-service --cluster <ECS PRIMARY CLUSTER NAME LIKE: FixEngineOnAws-Server-1-19-PrimaryECSCluster-9ADvGgfFh8Os> --service <ECS PRIMARY SERVICE NAME LIKE: FixEngineOnAws-Server-1-19-PrimaryECSService-wQDEsRdRkGue> --force-new-deployment

aws ecs update-service --cluster <ECS BACKUP  CLUSTER NAME LIKE: FixEngineOnAws-Server-1-19-FailoverECSCluster-2yATofglKCo9> --service <ECS BACKUP SERVICE NAME LIKE: FixEngineOnAws-Server-1-19-FailoverECSService-uQ0G9vUhT6H9> --force-new-deployment
