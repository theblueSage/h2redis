/*
*  Copyright (c) 2013, Jumptap (http://www.jumptap.com) All Rights Reserved.
*
*  Jumptap licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.jumptap.h2redis;

public class KeyMaker {
    private final String keyPrefix;
    private final String hashKeyPrefix;

    public KeyMaker(String keyPrefix, String hashKeyPrefix, String delimiter) {
        this.keyPrefix = keyPrefix + delimiter;
        this.hashKeyPrefix = hashKeyPrefix + delimiter;
    }

    public String key(String key) {
        if (keyPrefix != null && !keyPrefix.isEmpty()) {
            return keyPrefix + key;
        }
        else {
            return key;
        }
    }

    public String hkey(String key) {
        if (hashKeyPrefix != null && !hashKeyPrefix.isEmpty()) {
            return hashKeyPrefix + key;
        }
        else {
            return key;
        }
    }
}
