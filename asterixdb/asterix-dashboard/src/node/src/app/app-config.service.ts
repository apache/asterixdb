
import {throwError as observableThrowError } from 'rxjs';
/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

interface ENV_DATA {
  env: string
}

@Injectable()
export class ConfigService {

    private config: Object
    private env: Object

    constructor(private http: HttpClient) {}

    /**
     * Loads the environment config file first. Reads the environment variable from the file
     * and based on that loads the appropriate configuration file - development or production
     */
    load() {
        return new Promise((resolve, reject) => {
            let headers = new HttpHeaders({ 'Accept': 'application/json', 'Content-Type': 'application/json', 'DataType': 'application/json' });
            const HttpOptions = {
              headers: new HttpHeaders({
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'DataType': 'application/json',
              })
            }

            this.http.get('/config/env.json', HttpOptions)
              .subscribe({
                next(env_data: ENV_DATA) {
                  this.env = env_data;

                  this.http.get('/config/' + env_data.env + '.json')
                    .catch((error: any) => {
                      return observableThrowError(error.json().error || 'Server error');
                    })
                    .subscribe({
                      next(data) {
                        this.config = data;
                        resolve(true);
                      }
                    })
                }
              });
        });
    }

    /**
     * Returns environment variable based on given key
     *
     * @param key
    */
    getEnv(key: any) {
        return this.env[key];
    }

    /**
     * Returns configuration value based on given key
     *
     * @param key
    */
    get(key: any) {
        return this.config[key];
    }
}
