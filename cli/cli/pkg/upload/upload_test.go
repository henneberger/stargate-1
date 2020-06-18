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

package upload

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/datastax/stargate/cli/pkg/config"
	"github.com/datastax/stargate/cli/pkg/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type UploadSuite struct {
	suite.Suite
	client                 docker.Client
	dockerConfig           config.SGDockerConfig
	ServiceContainerName   string
	ServiceNetworkName     string
	CassandraContainerName string
}

func (suite *UploadSuite) SetupSuite() {
	client, err := docker.NewClient()
	if err != nil {
		log.Fatalf("unable to connect to docker %s", err)
	}
	dockerConfig, err := config.NewSGDockerConfig("v0.2.0", "3.11.6")
	if err != nil {
		log.Fatalf("unable to wire up configuration with '%s'", err)
	}
	rand.Seed(time.Now().UnixNano())
	testRun := rand.Int()
	suite.CassandraContainerName = fmt.Sprintf("%s%d", dockerConfig.CassandraContainerName(), testRun)
	suite.ServiceContainerName = fmt.Sprintf("%s%d", dockerConfig.ServiceContainerName(), testRun)
	suite.ServiceNetworkName = fmt.Sprintf("%s%d", dockerConfig.ServiceNetworkName(), testRun)
	err = client.StartCassandra(&docker.StartCassandraOptions{
		ContainerName:      suite.CassandraContainerName,
		DockerImageHost:    "",
		ImageName:          dockerConfig.CassandraImage(),
		ServiceNetworkName: suite.ServiceNetworkName,
	})

	if err != nil {
		log.Fatalf("unable to start cassandra %s", err)
	}
	time.Sleep(50 * time.Second)
	err = client.StartService(&docker.StartServiceOptions{
		CassandraURL:         suite.CassandraContainerName,
		ExposedPorts:         []string{"8080"},
		DockerImageHost:      "",
		ImageName:            dockerConfig.ServiceImage(),
		ServiceContainerName: suite.ServiceContainerName,
		ServiceNetworkName:   suite.ServiceNetworkName,
	})
	if err != nil {
		log.Fatalf("unable to start service %s", err)
	}
	suite.client = client
}

func (suite *UploadSuite) TearDownSuite() {
	//suite.client.Remove(suite.ServiceContainerName)
	suite.client.Remove(suite.CassandraContainerName)
	suite.client.RemoveNetwork(suite.ServiceNetworkName)
}

const validHost = "http://localhost:8080/v1/api/test/schema"
const validPath = "../../../../src/main/resources/schema.conf"

func (suite *UploadSuite) TestInvalidPathInput() {
	err := Upload("", validHost)
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestMalformedHostInput() {
	err := Upload(validPath, "\n")
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestBadHostInput() {
	err := Upload(validPath, "")
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestBadPathInput() {
	err := Upload("./upload_test.go", validHost)
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestValidInput() {
	err := Upload(validPath, validHost)
	assert.Nil(suite.T(), err)
}

func TestUpload(t *testing.T) {
	suite.Run(t, new(UploadSuite))
}
