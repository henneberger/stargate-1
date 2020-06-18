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
	"fmt"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh/terminal"
)

// EncryptCmd is a simple command for generating a encrypted password
var EncryptCmd = &cobra.Command{
	Short: "Encrypt a password with bcrypt",
	Long: `Encrypt a password with bcrypt

Running 'stargate encrypt' will prompt you to enter a password twice and if they match you will recieve a password hashed by
bcrypt, which you can then add to your stargate server.`,
	Use:     "encrypt",
	Example: "stargate encrypt",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Enter Password: ")
		firstPass, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Printf("unable to read terminal with error '%s' \n", err)
			os.Exit(1)
		}
		if len(firstPass) == 0 {
			fmt.Println("cannot process and empty password")
			os.Exit(1)
		}
		fmt.Println("Enter Password Again: ")
		secondPass, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Printf("unable to read terminal with error '%s'\n", err)
			os.Exit(1)
		}
		if len(secondPass) == 0 {
			fmt.Println("cannot process and empty password")
			os.Exit(1)
		}
		if string(firstPass) != string(secondPass) {
			fmt.Println("the first and second password entry do not match try again")
			os.Exit(1)
		}
		passBytes, err := bcrypt.GenerateFromPassword(firstPass, 12)
		if err != nil {
			fmt.Printf("unable to generate password hash with error '%s'\n", err)
			os.Exit(1)
		}
		fmt.Printf(`use the following hash in ONE of the following:
* SG_SERVICE_AUTH_PASS_HASH environment variable "export SG_SERVICE_AUTH_ENABLED=true; export SG_SERVICE_AUTH_PASS_HASH='%s'"
* add it to the defaults.conf auth.passwordHash section
* If using helm you can pass it to helm with "helm install --set stargate.authPasswordHash='%s' stargate-deploy ./charts/stargate"
`, string(passBytes), string(passBytes))
		fmt.Println(string(passBytes))
	},
}

func init() {
	rootCmd.AddCommand(EncryptCmd)
}
