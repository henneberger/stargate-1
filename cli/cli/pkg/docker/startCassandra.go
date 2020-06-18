// Copyright DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docker

import (
	"fmt"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
)

// StartCassandraOptions defines the input of Client.Start
type StartCassandraOptions struct {
	ContainerName      string
	DockerImageHost    string
	ImageName          string
	ServiceNetworkName string
	ExposedPorts       []string
}

// StartCassandra running docker image
func (client *Client) StartCassandra(opts *StartCassandraOptions) error {
	cli := client.cli
	ctx := client.ctx
	containerName := opts.ContainerName

	err := client.EnsureNetwork(opts.ServiceNetworkName)
	if err != nil {
		return err
	}
	//TODO check for status of existing containers
	if err := client.Remove(containerName); err != nil {
		fmt.Printf("removing container %s\n", containerName)
	}

	image, err := client.EnsureImage(opts.DockerImageHost, opts.ImageName)
	if err != nil {
		return err
	}

	containerConfig := container.Config{
		Image:        image,
		ExposedPorts: nat.PortSet{},
	}

	hostConfig := container.HostConfig{
		PortBindings: nat.PortMap{},
	}

	var empty struct{}
	for _, portValue := range opts.ExposedPorts {
		port, err := nat.NewPort("tcp", portValue)
		if err != nil {
			return err
		}
		containerConfig.ExposedPorts[port] = empty
		hostConfig.PortBindings[port] = []nat.PortBinding{
			{HostPort: port.Port()},
		}
	}

	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"stargate": {NetworkID: opts.ServiceNetworkName},
		},
	}

	resp, err := cli.ContainerCreate(ctx, &containerConfig, &hostConfig, &networkConfig, containerName)
	if err != nil {
		return fmt.Errorf("unable to create container name '%s' from image '%s' because of error from docker '%s'", containerName, opts.ImageName, err)
	}

	fmt.Println("Starting Cassandra. This might take a minute...")
	err = cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		return fmt.Errorf("unable to start container '%s' from image '%s' because of error from docker '%s'", containerName, opts.ImageName, err)
	}

	return client.Started(resp.ID, "Starting listening for CQL clients")
}
