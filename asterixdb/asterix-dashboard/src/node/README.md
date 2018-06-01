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

Install node and npm, any of the latest versions will do.

Run `npm install` to download all the dependency packages an recreate the node_modules directory.

## Development server

The development version uses the webpack proxy to avoid CORS problems in Angular see:  

`https://daveceddia.com/access-control-allow-origin-cors-errors-in-angular`. 

Please check `proxy.config.js` to see how it's configured.

Run `ng serve` or `npm start` for a dev server. Navigate to `http://localhost:4200/`. The app will automatically reload if you change any of the source files.

A technical document describing the internals and architecture exist, here:

`https://github.com/EmilioMobile/asterixdb-dashboard/blob/master/documents/AsterixDB%20Architecture%20v1.0.pdf?raw=true`

A brief user guide document describing how to use it, here:

`https://github.com/EmilioMobile/asterixdb-dashboard/blob/master/documents/AsterixDB%20User%20Guide%20v1.0.pptx?raw=true`
