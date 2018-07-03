#!/bin/bash
set -x
# If this script is run by intellij, the docker must be detached since the run window isn't a tty. Therefore the default is -d.
# Console output can be seen with docker logs -f <container_ID>.
# If no version is specified, a new image will be build tagged as ${USER}
USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble

package=rawrepo-record-service
cid_file="${package}.cid"
docker_image=docker-i.dbc.dk/rawrepo-record-service
version=${USER}
port=`id -u ${USER}`10
detached="-d"
while getopts "p:v:u" opt; do
    case "$opt" in
    "u" )
            detached=""
            ;;
    "p" )
            port=$OPTARG
            ;;
    "v" )
            version=$OPTARG
            ;;
    esac
done

if [ ! -d ${HOME}/.ocb-tools ]
then
    mkdir ${HOME}/.ocb-tools
fi

if [ "$version" = "${USER}" ]
then
	hop=`pwd`
    echo "Building ${package}"
	mvn clean package > /tmp/mvn.out.${USER}.${package}
	echo "Done building"
    docker build -t ${docker_image}:${USER} .
    cc=$?
    if [ ${cc} -ne 0 ]
    then
        echo "Couldn't build image"
        exit 1
    fi
fi

if [ -f ${HOME}/.ocb-tools/${cid_file} ]
then
    docker stop `cat ${HOME}/.ocb-tools/${cid_file}`
fi

#Find the correct outbound ip-address regardless of host configuration
if [ "$(uname)" == "Darwin" ]
then
    export HOST_IP=$(ip addr show | grep inet | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' | egrep -v '^127.0.0.1' | head -1)
else
    export HOST_IP=$( ip -o addr show | grep "inet " | cut -d: -f2- | cut -c2- | egrep -v "^docker|^br" | grep "$(ip route list | grep default | cut -d' ' -f5) " | cut -d' ' -f6 | cut -d/ -f1)
fi

rr_conn=`egrep rawrepo.jdbc.conn.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"/" -f3-`
rr_user=`egrep rawrepo.jdbc.conn.user ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
rr_pass=`egrep rawrepo.jdbc.conn.passwd ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
echo "Starting container"
container_id=`docker run -it ${detached} -p ${port}:8080 \
        -e RAWREPO_URL="${rr_user} ${rr_user}:${rr_pass}@${rr_conn}" \
		-e OPENAGENCY_URL="http://openagency.addi.dk/test_2.34/" \
		-e INSTANCE_NAME="${package}_${USER}_dev" \
		-e ADD_JVM_ARGS="-Xms2g" \
		 ${docker_image}:${version}`
cc=$?
if [ ${cc} -ne 0 ]
then
    echo "Couldn't start"
    exit 1
else
    echo ${container_id} > ${HOME}/.ocb-tools/${cid_file}
    echo "PORT: ${port}"
    echo "CID : ${container_id}"
    imageName=`docker inspect --format='{{(index .Name)}}' ${container_id} | cut -d"/" -f2`
    echo "NAME: ${imageName}"

    # Remove the recordservice.url line from testrun.properties to avoid multiple entries
    # For some reason sed doesn't work the same way on Linux and Mac...
    if [ "$(uname)" == "Darwin" ]
    then
        sed -i '' '/recordservice.url/d' ${HOME}/.ocb-tools/testrun.properties
    else
        sed -i '/recordservice.url/d' ${HOME}/.ocb-tools/testrun.properties
    fi
    RECORDSERVICE_PORT_8080=`docker inspect --format='{{(index (index .NetworkSettings.Ports "8080/tcp") 0).HostPort}}' ${imageName} `
    echo "recordservice.url = http://${HOST_IP}:${RECORDSERVICE_PORT_8080}" >> ${HOME}/.ocb-tools/testrun.properties
fi