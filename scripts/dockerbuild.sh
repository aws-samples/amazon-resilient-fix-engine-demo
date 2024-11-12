while [[ "$#" -gt 0 ]]
  do
    case $1 in
      --account-id)
        AWS_ACCOUNT_ID="$2"
        ;;
      --region)
        AWS_REGION="$2"
        ;;
      --ecr-repo-name)
        ECR_REPO_NAME="$2"
        ;;
      --engine-role)
        ENGINE_ROLE="$2"
        ;;
    esac
    shift
  done

# Generates IMDS v2 Token
IMDS_TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`

# Uses Current AWS Region if Not Supplied as Argument (Metadata Service & IAM Role Required)
AWS_REGION=${AWS_REGION:-$(curl -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq .region -r)}

# Uses Current AWS Account ID if Not Supplied as Argument (Metadata Service & IAM Role Required)
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-$(curl -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq .accountId -r)}

aws ecr get-login-password --region $AWS_REGION | sudo docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

gradle build
sudo service docker start
sudo docker build -t fixengineonaws --file Dockerfile$ENGINE_ROLE .

sudo docker tag fixengineonaws:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_NAME:latest

sudo docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_NAME:latest
