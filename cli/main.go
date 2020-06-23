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

package main

import (
	"github.com/datastax/stargate/cli/cmd"
)

//defaultSGVersion is overriden at build time by the pom.xml version during a tag push
var defaultSGVersion = "v0.3.1"
var defaultCassandraVersion = "3.11.6"

func main() {
	cmd.Execute(defaultSGVersion, defaultCassandraVersion)
}
