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

package cmd

import (
	"errors"

	"github.com/datastax/stargate/cli/pkg/config"
	"github.com/datastax/stargate/cli/pkg/docker"

	"github.com/spf13/cobra"
)

// ServiceCmd is the parent command for controlling a local stargate instance
var ServiceCmd = &cobra.Command{
	Short: "Start a local, dockerized service",
	Long: `Start a local, dockerized service

Running 'stargate service -c start' will start both a stargate server and cassandra instance to back it.
Stopping or removing with the cassandra flag will also stop and remove the local cassandra instance.`,
	Use:     "service (start|stop|remove)",
	Example: "stargate service start",
}

var withCassandra bool

// StopServiceCmd stops a local instance
var StopServiceCmd = &cobra.Command{
	Short:   "Stop a local stargate service",
	Long:    `Stop a local stargate service`,
	Use:     "stop",
	Example: "stargate service stop",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		dockerConfig, err := config.NewSGDockerConfig(serviceVersion, cassandraVersion)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		if withCassandra {
			err = client.Stop(dockerConfig.CassandraContainerName())
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}
		err = client.Stop(dockerConfig.ServiceContainerName())
		if err != nil {
			cmd.PrintErrln(err)
		} else {
			cmd.Println("Success!")
		}
	},
}

// RemoveServiceCmd force removes a local instance
var RemoveServiceCmd = &cobra.Command{
	Short:   "Force remove a local stargate service",
	Long:    `Force remove a local stargate service`,
	Use:     "remove",
	Example: "stargate service remove",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		dockerConfig, err := config.NewSGDockerConfig(serviceVersion, cassandraVersion)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		if withCassandra {
			err = client.Remove(dockerConfig.CassandraContainerName())
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}
		err = client.Remove(dockerConfig.ServiceContainerName())
		if err != nil {
			cmd.PrintErrln(err)
		} else {
			cmd.Println("Success!")
		}
	},
}

// StartServiceCmd starts a local instance
var StartServiceCmd = &cobra.Command{
	Short:   "Start a local, dockerized service server",
	Long:    `Start a local, dockerized service server`,
	Use:     "start [CASSANDRA_HOST]",
	Example: "stargate service start",
	Args:    cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dockerConfig, err := config.NewSGDockerConfig(serviceVersion, cassandraVersion)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		if withCassandra && len(args) == 1 {
			cmd.PrintErrln(errors.New("If you are starting with --with-cassandra, you should not specify a cassandra host"))
			return
		}
		cassandraURL := dockerConfig.CassandraContainerName()
		if len(args) == 1 {
			cassandraURL = args[0]
		}
		if withCassandra {
			err = client.StartCassandra(&docker.StartCassandraOptions{
				//"" means docker.io
				DockerImageHost:    "",
				ImageName:          dockerConfig.CassandraImage(),
				ServiceNetworkName: dockerConfig.ServiceNetworkName(),
				ContainerName:      dockerConfig.CassandraContainerName(),
			})
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}

		err = client.StartService(&docker.StartServiceOptions{
			CassandraURL: cassandraURL,
			ExposedPorts: []string{"8080"},
			//"" means docker.io
			DockerImageHost:      "",
			ImageName:            dockerConfig.ServiceImage(),
			ServiceNetworkName:   dockerConfig.ServiceNetworkName(),
			ServiceContainerName: dockerConfig.ServiceContainerName(),
		})
		if err != nil {
			cmd.PrintErrln(err)
		} else {
			cmd.Println("Success!")
		}
	},
}

func init() {
	rootCmd.AddCommand(ServiceCmd)

	ServiceCmd.AddCommand(StopServiceCmd)
	ServiceCmd.AddCommand(RemoveServiceCmd)
	ServiceCmd.AddCommand(StartServiceCmd)

	ServiceCmd.PersistentFlags().StringVarP(&serviceVersion, "stargate-version", "s", defaultServiceVersion, "the docker image tag to use for stargate")
	ServiceCmd.PersistentFlags().StringVarP(&cassandraVersion, "cassandra-version", "t", defaultCassandraVersion, "the docker image tag to use for cassandra")
	ServiceCmd.PersistentFlags().BoolVarP(&withCassandra, "with-cassandra", "c", false, "apply the stargate actions to a cassandra container as well")
}
