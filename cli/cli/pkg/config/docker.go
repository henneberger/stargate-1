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
package config

import "fmt"

//SGDockerConfig provides the information to coordinated docker
type SGDockerConfig struct {
	cassandraImage         string
	cassandraVersion       string
	cassandraContainerName string
	serviceContainerName   string
	serviceImage           string
	serviceNetwork         string
	serviceVersion         string
}

//NewSGDockerConfig starts a new instance with default values initialized
func NewSGDockerConfig(serviceVersion, cassandraVersion string) (*SGDockerConfig, error) {
	if serviceVersion == "" {
		return &SGDockerConfig{}, fmt.Errorf("service version was empty this is invalid")
	}
	if cassandraVersion == "" {
		return &SGDockerConfig{}, fmt.Errorf("cassandra version was empty this is invalid")
	}
	return &SGDockerConfig{
		serviceContainerName:   "stargate",
		serviceImage:           "datastax/stargate",
		serviceVersion:         serviceVersion,
		serviceNetwork:         "stargate",
		cassandraContainerName: "stargate-cassandra",
		cassandraImage:         "cassandra",
		cassandraVersion:       cassandraVersion,
	}, nil
}

//ServiceNetworkName provides a bridge network for cassandra and stargate to communicate
func (sg *SGDockerConfig) ServiceNetworkName() string {
	return sg.serviceNetwork
}

//CassandraImage is the full image and version of the Apache Cassandra docker image
func (sg *SGDockerConfig) CassandraImage() string {
	return fmt.Sprintf("%s:%s", sg.cassandraImage, sg.cassandraVersion)
}

//ServiceImage is the full image and version of the stargate service docker image
func (sg *SGDockerConfig) ServiceImage() string {
	return fmt.Sprintf("%s:%s", sg.serviceImage, sg.serviceVersion)
}

//CassandraContainerName is the name of the apache cassandar container name in docker
func (sg *SGDockerConfig) CassandraContainerName() string {
	return sg.cassandraContainerName
}

//ServiceContainerName is the name of the stargate container name  in docker
func (sg *SGDockerConfig) ServiceContainerName() string {
	return sg.serviceContainerName
}
