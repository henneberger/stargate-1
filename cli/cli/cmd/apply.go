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
	"os"

	"github.com/PuerkitoBio/purell"
	"github.com/datastax/stargate/cli/pkg/upload"

	"github.com/spf13/cobra"
)

func SchemaUrl(url string, name string) string {
    return url+"/v1/api/"+name+"/schema"
}

// Apply sends a schema to server and print output for a user
func Apply(cmd *cobra.Command, name, path, url string) error {
	if url == "" {
		url = upload.Host
	}

	return upload.Upload(path, SchemaUrl(url, name))
}

// ApplyWithLog sends a schema to server and print output for a user
func ApplyWithLog(cmd *cobra.Command, name, path, url string) error {
	if url == "" {
		url = upload.Host
	}
	err := Apply(cmd, name, path, url)

	if err != nil {
		cmd.PrintErrln("Failed to apply schema!")
		cmd.PrintErrln(err.Error())
	} else {
		endpointURL := purell.MustNormalizeURLString(SchemaUrl(url, name), purell.FlagsUnsafeGreedy)
		cmd.Println("Endpoint created at", endpointURL)
		cmd.Printf("\nView your API at: %s/api-docs/%s/swagger/\n\n", url, name)
	}
	return err
}

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Short: "Apply schema to a stargate server",
	Long: `Apply schema to a stargate server to create or update a database
	
Apply takes two required parameters:
	name: the name of the database you're creating
	path: the path to schema file

And one optional parameter:
	host: the host and port of the running Stargate instance, the default being a local instance`,
	Use:     "apply name path [host]",
	Example: "stargate apply todo ./todo.conf server.stargate.com:8080",
	Args:    cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		name, path := args[0], args[1]
		var url string
		if len(args) == 3 {
			url = args[2]
		}

		err := ApplyWithLog(cmd, name, path, url)
		if err != nil {
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)
}
