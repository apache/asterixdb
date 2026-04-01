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

# AsterixDB Web Administration Console

AsterixDB Administration Console is an Angular 5 HTML/CSS/Typescript web application using predictable state containers, immutable data updates with Redux/NGRx frameworks, observable patterns, and standard Open Sourced UI widgets for data, metadata manipulation and visualization through SQL++ AsterixDB Query language.

The main goal is create a baseline solution with unopinionated UI/UX design and SW architecture with two differentiated isolated, indepent minimal coupled levels between the UI components and application core or internal core, easily extensible and scalable by the AsterixDB community.

## Development

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 1.4.9.

## Installation

Navigate to the project directory:
```
cd /asterixdb/asterixdb/asterix-dashboard/src/node
```

Install node and npm, any of the latest versions will do.

Run `npm install` to download all the dependency packages an recreate the node_modules directory.

## Development server

The development version uses the webpack proxy to avoid CORS problems in Angular see:  

`https://daveceddia.com/access-control-allow-origin-cors-errors-in-angular`. 

Please check `proxy.config.js` to see how it's configured.

## Running the Application

1. Start the development server with proxy configuration:
   ```
   ng serve --proxy-config proxy.config.js
   ```
   This will start a dev server at port 4200. Navigate to `http://localhost:4200/` in your web browser to access the application.

2. Ensure the backend server is running to get results on the proxy server.
   The backend server can be started using `AsterixServerIntegrationUtil` or `AsterixHyracksIntegrationUtil`.

## Troubleshooting

If you encounter the following error:
```
Error: error:0308010C:digital envelope routines::unsupported
```

Run the following command before starting the server:
```
export NODE_OPTIONS=--openssl-legacy-provider
```

Then try running the server again:
```
ng serve --proxy-config proxy.config.js
```
