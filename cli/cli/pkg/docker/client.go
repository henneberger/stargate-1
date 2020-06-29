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
	"context"

	"github.com/docker/docker/client"
)

// Client is a container for a docker client
type Client struct {
	ctx context.Context
	cli *client.Client
}

// NewClient returns a new Cli struct
func NewClient() (Client, error) {
	ctx := context.Background()
	cli, err := client.NewEnvClient()
	if err != nil {
		return Client{}, err
	}

	return Client{ctx, cli}, nil
}
