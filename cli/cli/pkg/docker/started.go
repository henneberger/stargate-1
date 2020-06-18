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
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/pkg/stdcopy"
)

type findWriter struct {
	trigger string
	bytes   []byte
	found   bool
}

func (w *findWriter) Write(p []byte) (n int, err error) {
	w.bytes = append(w.bytes, p...)
	if strings.Contains(string(w.bytes), w.trigger) {
		w.found = true
		return 0, errors.New("found")
	}
	return len(p), nil
}

func newFindWriter(trigger string) findWriter {
	return findWriter{trigger, make([]byte, 0), false}
}

// Started function waits for a docker image to start
func (client *Client) Started(containerID, trigger string) error {
	cli := client.cli
	ctx := client.ctx

	logs, err := cli.ContainerLogs(ctx, containerID, types.ContainerLogsOptions{
		Follow:     true,
		ShowStderr: false,
		ShowStdout: true,
	})
	if err != nil {
		return err
	}
	defer logs.Close()

	stdOut := newFindWriter(trigger)
	errOut := newFindWriter(trigger)
	stdcopy.StdCopy(&stdOut, &errOut, logs)

	if !stdOut.found && !errOut.found {
		t := time.Now().Unix()
		fileNameOut := fmt.Sprintf("/tmp/stargate-out-%v-%v.log", containerID, t)
		fileNameErr := fmt.Sprintf("/tmp/stargate-err-%v-%v.log", containerID, t)
		ioutil.WriteFile(fileNameOut, stdOut.bytes, 0644)
		ioutil.WriteFile(fileNameErr, errOut.bytes, 0644)
		return fmt.Errorf("Failed to start container. For more context see:\n%v\n%v", fileNameOut, fileNameErr)
	}

	return nil
}
