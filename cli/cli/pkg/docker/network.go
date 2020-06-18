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
)

func (client *Client) networkExists(name string) (bool, error) {
	cli := client.cli
	ctx := client.ctx
	networks, err := cli.NetworkList(ctx, types.NetworkListOptions{})
	if err != nil {
		return false, err
	}
	networkFound := false
	for _, n := range networks {
		if n.Name == name {
			networkFound = true
			break
		}
	}
	return networkFound, nil
}

// EnsureNetwork checks to see if the stargate bridge network exists and creates it if it doesnt
func (client *Client) EnsureNetwork(networkName string) error {
	cli := client.cli
	ctx := client.ctx
	networkFound, err := client.networkExists(networkName)
	if err != nil {
		return err
	}

	if !networkFound {
		fmt.Printf("creating network '%s'\n", networkName)
		_, err := cli.NetworkCreate(ctx, networkName, types.NetworkCreate{})
		if err != nil {
			return fmt.Errorf("unable to create network '%s' with error '%s'", networkName, err)
		}
	}
	return nil
}

// RemoveNetwork checks to see if the stargate bridge network exists and removes it if it does
func (client *Client) RemoveNetwork(networkName string) error {
	cli := client.cli
	ctx := client.ctx
	networkFound, err := client.networkExists(networkName)
	if err != nil {
		return err
	}

	if networkFound {
		fmt.Printf("removeing network '%s'\n", networkName)
		err := cli.NetworkRemove(ctx, networkName)
		if err != nil {
			return fmt.Errorf("unable to remove network '%s' with error '%s'", networkName, err)
		}
	}
	return nil
}
