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

// StartServiceOptions defines the input of Client.Start
type StartServiceOptions struct {
	CassandraURL          string
	ExposedPorts          []string
	DockerImageHost       string
	ImageName             string
	ServiceContainerName string
	ServiceNetworkName   string
}

// StartService running docker image
func (client *Client) StartService(opts *StartServiceOptions) error {
	cli := client.cli
	ctx := client.ctx
	containerName := opts.ServiceContainerName
	err := client.EnsureNetwork(opts.ServiceNetworkName)
	if err != nil {
		return err
	}

	client.Remove(containerName)

	image, err := client.EnsureImage(opts.DockerImageHost, opts.ImageName)
	if err != nil {
		return err
	}

	containerConfig := container.Config{
		Image:        image,
		ExposedPorts: nat.PortSet{},
		Env: []string{
			`SG_CASS_CONTACT_POINTS=` + opts.CassandraURL + `:9042`,
		},
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
		return err
	}

	fmt.Println("Starting Stargate...")
	err = cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}

	return client.Started(resp.ID, "org.eclipse.jetty.server.Server - Started")
}
