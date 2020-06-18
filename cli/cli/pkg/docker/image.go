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
	"os/exec"
)

// EnsureImage makes sure that the image we need is present and returns the correct name
func (client *Client) EnsureImage(dockerHost, imageName string) (string, error) {
	var fullImage = ""
	//dockerHost = "" signifies docker.io
	if dockerHost == "" {
		fullImage = imageName
	} else {
		fullImage = fmt.Sprintf("%s/%s", dockerHost, imageName)
	}
	fmt.Println("Pulling image:", fullImage)
	pullCmd := exec.Command("docker", "pull", fullImage)
	_, err := pullCmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("unable to pull '%s' due to '%s' from the docker command", fullImage, err.Error())
	}
	return imageName, nil
}
