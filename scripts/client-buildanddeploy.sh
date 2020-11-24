aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <ECR BASE URL LIKE: 123456789123.dkr.ecr.us-east-1.amazonaws.com>

docker build --file DockerfileClient -t jlt/fixengineonaws .

docker tag jlt/fixengineonaws:latest <ECR IMAGE URL LIKE: 123456789123.dkr.ecr.us-east-1.amazonaws.com/jlt/fixengineonaws:latest>

docker push <ECR IMAGE URL LIKE: 123456789123.dkr.ecr.us-east-1.amazonaws.com/jlt/fixengineonaws:latest>

aws ecs update-service --cluster <ECS PRIMARY CLUSTER NAME LIKE: FixEngineOnAws-Client-1-19-PrimaryECSCluster-NMMTm9dTCiYX>  --service <ECS PRIMARY SERVICE NAME LIKE: FixEngineOnAws-Client-1-19-PrimaryECSService-nWcoxV10rnCd> --force-new-deployment

aws ecs update-service --cluster <ECS BACKUP  CLUSTER NAME LIKE: FixEngineOnAws-Client-1-19-FailoverECSCluster-xxoCOavLYQi1>  --service <ECS BACKUP SERVICE NAME LIKE: FixEngineOnAws-Client-1-19-FailoverECSService-Kmmfz8fVMYKy> --force-new-deployment