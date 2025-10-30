#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <key_file> <ec2_public_ip>"
    exit 1
fi

KEY_FILE="$1"
EC2_IP="$2"

echo "Deploying to EC2 instance: $EC2_IP"

# Copy files
scp -i "$KEY_FILE" api_server.py ec2-user@"$EC2_IP":~
scp -i "$KEY_FILE" requirements.txt ec2-user@"$EC2_IP":~

# Install dependencies and start server
ssh -i "$KEY_FILE" ec2-user@"$EC2_IP" << 'EOF'
  pip3 install -r requirements.txt || pip3 install boto3

  # Kill existing server if running
  pkill -f "api_server.py" || true

  # Set DynamoDB table name for the server
  export DDB_TABLE=arxiv-papers

  # Start server in background
  nohup python3 api_server.py --port 8080 > server.log 2>&1 &

  echo "Server started. Check with: curl http://localhost:8080/papers/recent?category=cs.LG"
EOF

echo "Deployment complete"
echo "Test with: curl http://$EC2_IP:8080/papers/recent?category=cs.LG"
