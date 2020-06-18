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
	"errors"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/PuerkitoBio/purell"
)

// Upload posts the contents of a file to an url
func Upload(path string, url string) error {
	url, err := purell.NormalizeURLString(url, purell.FlagsUnsafeGreedy)
	if err != nil {
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	resp, err := http.Post(url, "application/hocon", file)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		message, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New("Error:\n" + string(message))
	}

	return nil
}
