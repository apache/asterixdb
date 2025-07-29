<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
!-->

# Running AsterixDB via Podman #

> **Note:** Make sure to install and set up Podman before proceeding. This setup will not work without it. For installation instructions, [see the links section](#links).

## <a id="toc">Table of Contents</a> ##

* [Building the Image](#build)
* [Pulling the Image](#pull)
* [Running AsterixDB](#run)
* [Using Detached Mode](#detached)
* [Stopping the Container](#stop)
* [Using a Custom Config](#config)
* [Pushing Images](#registry)
* [Useful Links](#links)
* [Available Configuration Parameters](ncservice.html#Parameters)


# <a id="build">Building the Image</a>

To build the Podman-compatible container image for AsterixDB, use the following Maven command:
Pre-requisite install and setup Podman, [see](#links). Or else this will not work
```bash
mvn clean package -Ppodman.img
```

This will build the server, copy required resources, and create an image using the Podman engine. The image will be tagged as `apache/asterixdb:latest` by default.

If the base image is not available locally, ensure the build pulls it:

```bash
podman build --pull -t apache/asterixdb .
```


# <a id="pull">Pulling the Image</a>

If you only want to run AsterixDB and do not need to build it from source, you can simply pull the prebuilt image from the registry:

```bash
podman pull apache/asterixdb:latest
```

This will retrieve the latest tagged image. You can use it directly with `podman run` without needing to compile or build anything yourself.

For reproducible deployments, prefer using versioned tags (e.g., `apache/asterixdb:0.9.10`) instead of `latest`.


# <a id="run">Running AsterixDB</a>

You can run AsterixDB in standalone mode using the following command:

```bash
podman run \
  -p 19001:19001 \
  -p 19002:19002 \
  -p 19004:19004 \
  -p 19006:19006 \
  -p 19007:19007 \
  -p 1098:1098 \
  -p 1099:1099 \
  apache/asterixdb
```

This will start AsterixDB with a default configuration exposing the Web UI, REST API, Node Controller etc. The Web interface will be available at: [http://localhost:19006](http://localhost:19006)

# <a id="detached">Using Detached Mode</a>

To run AsterixDB in the background (detached mode):

```bash
podman run -d \
  -p 19001:19001 \
  -p 19002:19002 \
  -p 19004:19004 \
  -p 19006:19006 \
  -p 19007:19007 \
  -p 1098:1098 \
  -p 1099:1099 \
  apache/asterixdb
```

> **Tip:** Use `--rm` with `podman run` to automatically clean up the container after it stops.  
> This is especially useful for development, testing, and teaching scenarios where containers are short-lived and don't need to persist.
> 
> **Tip:** Use `--name` with `podman run` to assign a custom name to your container.  
> This makes it easier to reference the container in commands like `podman logs`, `podman exec`, or `podman restart` without needing to remember the container ID.

You can verify the container is running:

```bash
podman ps
```
To view the container’s output logs, use:

```bash
podman logs <container-name-or-id>
```

You can use either the container’s name (if you specified one using --name) or the container ID shown in podman ps.
This is useful for basic debugging.

You can enter a shell inside the running container with:

```bash
podman exec -it <container-name-or-id> sh
```

  Once inside, the default home directory is:

```bash
/var/lib/asterix
```

# <a id="stop">Stopping the Container</a>

To stop the running AsterixDB container:

```bash
podman stop <container-id>
```

You can find the container ID using `podman ps`.


# <a id="config">Using a Custom Config</a>

You can override the default configuration by mounting your own configuration file and passing it to the container:

```bash
podman run \
  -v $(pwd)/my-conf:/config \
  -p 19001:19001 -p 19002:19002 -p 19004:19004 -p 19006:19006 -p 19007:19007 -p 1098:1098 -p 1099:1099 \
  apache/asterixdb \
  -config-file /config/cc.conf
```

The `-v $(pwd)/my-conf:/config` flag mounts a local directory named `my-conf` (located in your current working directory) into the container at `/config`. This allows AsterixDB to use your custom configuration file (`cc.conf`) without modifying the container. Make sure `my-conf/cc.conf` exists before running the command.
All available parameters and their usage can be found [here](ncservice.html#Parameters)

# <a id="registry">Pushing Images</a>

You can tag and push your local image to a container registry. Replace `docker.io/your-username` with your actual registry path (e.g., Docker Hub, GitHub Container Registry, etc.). The `:custom` tag indicates a custom version of the image you built locally — you can name it anything meaningful, such as `:dev` or `:2025-04-11`:
```bash
podman tag apache/asterixdb docker.io/your-username/asterixdb:custom
podman push docker.io/your-username/asterixdb:custom
```

Use version tags (e.g., `apache/asterixdb:0.9.10`) for official releases instead of `latest` to ensure reproducibility.


# <a id="links">Useful Links</a>

- [Podman Installation Guide](https://podman.io/docs/installation)
- [Podman Documentation](https://docs.podman.io/en/latest/)
- [Available Configuration Parameters](ncservice.html#Parameters)

