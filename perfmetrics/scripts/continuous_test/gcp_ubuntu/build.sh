#i!/bin/bash
set -e
sudo apt-get update
# echo Installing fio
# sudo apt-get install fio -y
# echo Installing gcsfuse
# GCSFUSE_VERSION=0.41.1
# curl -L -O https://github.com/GoogleCloudPlatform/gcsfuse/releases/download/v$GCSFUSE_VERSION/gcsfuse_"$GCSFUSE_VERSION"_amd64.deb
# sudo dpkg --install gcsfuse_"$GCSFUSE_VERSION"_amd64.deb
cd "${KOKORO_ARTIFACTS_DIR}/github/gcsfuse/perfmetrics/scripts"
echo Mounting gcs bucket
mkdir -p gcs
GCSFUSE_FLAGS="--implicit-dirs --max-conns-per-host 100 --disable-http2"
BUCKET_NAME=gcs-fuse-dashboard-fio
MOUNT_POINT=gcs
COMMAND=gcsfuse $GCSFUSE_FLAGS $BUCKET_NAME $MOUNT_POINT
script --command "$COMMAND" --log-out logfile
#gcsfuse $GCSFUSE_FLAGS $BUCKET_NAME $MOUNT_POINT
echo printing logfile
cat logfile
chmod +x build.sh
# ./build.sh 


# #i!/bin/bash
# set -e
# sudo apt-get update
# echo Installing fio
# sudo apt-get install fio -y
# echo Installing gcsfuse
# curl -L -O https://github.com/GoogleCloudPlatform/gcsfuse/releases/download/v0.41.1/gcsfuse_0.41.1_amd64.deb
# sudo dpkg --install gcsfuse_0.41.1_amd64.deb
# cd "${KOKORO_ARTIFACTS_DIR}/github/gcsfuse/perfmetrics/scripts"
# echo Mounting gcs bucket
# mkdir gcs
# gcsfuse --implicit-dirs --max-conns-per-host 100 --disable-http2 gcs-fuse-dashboard-fio gcs 
# chmod +x build.sh
# ./build.sh
