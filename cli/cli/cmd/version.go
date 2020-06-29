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
	"strings"

	"github.com/spf13/cobra"
)

//versionCmd outputs current command version
var versionCmd = &cobra.Command{
	Short:   "Version output",
	Long:    `Full version of stargate command`,
	Use:     "version",
	Example: "stargate version",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		//this is a bit of a hack atm, should be decoupling this for the future
		cleanedVersion := strings.ReplaceAll(defaultServiceVersion, "v", "")
		cmd.Printf("stargate version: %s\n", cleanedVersion)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
