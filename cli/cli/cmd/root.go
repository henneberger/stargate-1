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
	"crypto/tls"
	"fmt"
	"net/http"
	"os"

	"github.com/datastax/stargate/cli/pkg/config"
	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string
var cassandraVersion string
var serviceVersion string

var defaultCassandraVersion string
var defaultServiceVersion string
var verbose bool
var insecureSSL bool
var dockerConfig *config.SGDockerConfig

var rootCmd = &cobra.Command{
	Use:   "stargate",
	Short: "A toolbox for working with Stargate",
	Long:  `Welcome to Stargate, the easy query layer for your NoSQL database.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if verbose {
			fmt.Println("WARNING verbose enabled")
		}
		if insecureSSL {
			fmt.Println("WARNING disabling SSL validation because -i flag was used")
			http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(newDefaultServiceVersion, newDefaultCassandraVersion string) {
	defaultCassandraVersion = newDefaultCassandraVersion
	defaultServiceVersion = newDefaultServiceVersion
	cassandraVersion = newDefaultCassandraVersion
	serviceVersion = newDefaultServiceVersion
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable verbose mode")
	rootCmd.PersistentFlags().BoolVarP(&insecureSSL, "ignore-ssl-sec", "i", false, "do not validate ssl certs, you can use this in testing with self signed certs but it is not safe for production")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Find home directory.
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Search config in home directory with name ".stargate" (without extension).
	viper.AddConfigPath(home)
	viper.SetConfigName(".stargate")

	viper.AutomaticEnv() // read in environment variables that match
}
