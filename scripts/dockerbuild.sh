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
# Uses Current AWS Region if Not Supplied as Argument (Metadata Service & IAM Role Required)
AWS_REGION=${AWS_REGION:-$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | grep region | awk -F\" '{print $4}')}
# Uses Current AWS Account ID if Not Supplied as Argument (Metadata Service & IAM Role Required)
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId)}
aws ecr get-login-password --region $AWS_REGION | sudo docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
gradle build
sudo service docker start
sudo docker build -t fixengineonaws --file Dockerfile$ENGINE_ROLE
sudo docker tag fixengineonaws:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY_NAME:latest
sudo docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$ECR_REPOSITORY_NAME:latest