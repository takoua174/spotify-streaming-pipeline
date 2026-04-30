. Copy the file into the container:

Bash

docker cp run_all_streams.sh spark-master:/opt/pipeline/processing/streaming/

2. Log into the container as the root user:

Bash

docker exec -u root -it spark-master bash

Part 2: Run these inside the Docker container

Once your prompt changes to root@7e163525b59e:/opt/spark/work-dir#, run these commands one by one:

3. Go to the streaming folder:

Bash

cd /opt/pipeline/processing/streaming

4. Fix the Windows line endings:

Bash

sed -i 's/\r$//' run_all_streams.sh

5. Give the script execution permissions:

Bash

chmod +x run_all_streams.sh

6. Run the script!

Bash

./run_all_streams.sh



clear checkpoint :
docker exec -it spark-master rm -rf /tmp/checkpoints_*