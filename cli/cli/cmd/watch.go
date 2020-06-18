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
	"io/ioutil"
	"log"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
)

func applyUpdate(cmd *cobra.Command, name, path, url string, ready chan bool) {
	ApplyWithLog(cmd, name, path, url)
	ready <- true
}

func fileUpdate(cmd *cobra.Command, name, path, url string, events chan fsnotify.Event) {
	newUpdate, waiting := false, false
	ready := make(chan bool)
	for {
		select {
		case event := <-events:
			if event.Op&fsnotify.Write == fsnotify.Write {
				if waiting {
					newUpdate = true
				} else {
					waiting = true
					go applyUpdate(cmd, name, path, url, ready)
				}
			}
		case <-ready:
			if newUpdate {
				newUpdate = false
				go applyUpdate(cmd, name, path, url, ready)
			} else {
				waiting = false
			}
		}
	}
}

func fileError(cmd *cobra.Command, done chan bool, Errors chan error) {
	for err := range Errors {
		cmd.PrintErrln(err)
	}
	done <- true
}

// WatchCmd represents the watch command
var WatchCmd = &cobra.Command{
	Short: "Watch a schema file and apply schema to a Stargate server",
	Long: `Watch a schema file and apply schema to a Stargate server
	
	if a schema file doesn't exist, an empty one will be created for you`,
	Use:     "watch name path [host]",
	Example: "stargate watch todo ./todo.conf http://server.stargate.com:8080",
	Args:    cobra.MinimumNArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Fatal(err)
		}
		defer watcher.Close()

		name, path, url := args[0], args[1], args[2]

		_, err = os.Stat(path)
		if err != nil {
			emptySchema := []byte("entities {}")
			ioutil.WriteFile(path, emptySchema, 0644)
		} else {
			ApplyWithLog(cmd, name, path, url)
		}

		done := make(chan bool)

		go fileUpdate(cmd, name, path, url, watcher.Events)

		err = watcher.Add(path)
		if err != nil {
			cmd.PrintErr(err)
			os.Exit(1)
		}
		<-done
	},
}

func init() {
	rootCmd.AddCommand(WatchCmd)
}
