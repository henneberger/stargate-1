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
	"github.com/datastax/stargate/cli/pkg/config"
	"github.com/datastax/stargate/cli/pkg/docker"

	"github.com/spf13/cobra"
)

// devCmd represents the apply command
var devCmd = &cobra.Command{
	Short: "Work with a local Stargate stack",
	Long: `Control a local Stargate stack (Cassandra, Stargate, filewatchers for an easy local experience)

This is the same as running:
stargate service start --withCassandra
stargate watch [FILE]`,
	Use:     "dev (start|stop|remove)",
	Example: "stargate dev start",
}

var stopDevCmd = &cobra.Command{
	Short:   "Stop a local stargate stack",
	Long:    `Stop a local stargate stack`,
	Use:     "stop",
	Example: "stargate dev stop",
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

var removeDevCmd = &cobra.Command{
	Short:   "Force remove a local stargate stack",
	Long:    `Force remove a local stargate stack`,
	Use:     "remove",
	Example: "stargate dev remove",
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

var startDevCmd = &cobra.Command{
	Short:   "Start a local stargate stack",
	Long:    `Start a local stargate stack`,
	Use:     "start [NAME] [PATH]",
	Example: "stargate dev start",
	Args:    cobra.ExactArgs(2),
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

		err = client.StartCassandra(&docker.StartCassandraOptions{
			DockerImageHost:    "", //"" means docker.io
			ImageName:          dockerConfig.CassandraImage(),
			ContainerName:      dockerConfig.CassandraContainerName(),
			ServiceNetworkName: dockerConfig.ServiceNetworkName(),
		})
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		err = client.StartService(&docker.StartServiceOptions{
			CassandraURL:         dockerConfig.CassandraContainerName(),
			ExposedPorts:         []string{"8080"},
			DockerImageHost:      "", //"" means docker.io
			ImageName:            dockerConfig.ServiceImage(),
			ServiceContainerName: dockerConfig.ServiceContainerName(),
			ServiceNetworkName:   dockerConfig.ServiceNetworkName(),
		})

		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		cmd.Println("Services started!")

		WatchCmd.Run(cmd, append(args, "http://localhost:8080"))
	},
}

func init() {
	rootCmd.AddCommand(devCmd)

	devCmd.AddCommand(stopDevCmd)
	devCmd.AddCommand(removeDevCmd)
	devCmd.AddCommand(startDevCmd)

	devCmd.PersistentFlags().StringVarP(&serviceVersion, "stargate-version", "s", defaultServiceVersion, "the docker image tag to use for stargate")
	devCmd.PersistentFlags().StringVarP(&cassandraVersion, "cassandra-version", "t", defaultCassandraVersion, "the docker image tag to use for cassandra")
}
