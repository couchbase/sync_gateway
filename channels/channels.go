//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channels

import "regexp"

var kValidChannelRegexp *regexp.Regexp

func init() {
	var err error
	kValidChannelRegexp, err = regexp.Compile(`^([-_.@\p{L}\p{Nd}]+|\*)$`)
	if err != nil {panic("Bad IsValidChannel regexp")}
}

func IsValidChannel(channel string) bool {
	return kValidChannelRegexp.MatchString(channel)
}

// Removes duplicates and invalid channel names.
// If 'starPower' is false, channel "*" is ignored.
// If it's true, channel "*" causes the output to be simply ["*"].
func SimplifyChannels(channels []string, starPower bool) []string {
	if len(channels) == 0 {
		return channels
	}
	result := make([]string, 0, len(channels))
	found := map[string]bool{}
	for _,ch := range channels {
		if !IsValidChannel(ch) {
			continue
		} else if ch == "*" {
			if starPower {
				return []string{"*"}
			}
		} else if !found[ch] {
			found[ch] = true
			result = append(result, ch)
		}
	}
	return result
}

// Returns true if the list contains the given channel name.
// A nil list is allowed and treated as an empty array.
func ContainsChannel(list []string, channelName string) bool {
	if list != nil {
		for _, item := range list {
			if item == channelName {
				return true
			}
		}
	}
	return false
}
