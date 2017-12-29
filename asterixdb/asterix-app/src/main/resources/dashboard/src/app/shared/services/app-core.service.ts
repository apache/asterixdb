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
import { Store } from '@ngrx/store';
import * as dataverseActions from '../../shared/actions/dataverse.actions'
import * as datasetActions from '../../shared/actions/dataset.actions'
import * as datatypesActions from '../../shared/actions/datatype.actions'
import * as indexesActions from '../../shared/actions/index.actions'
import * as metadataActions from '../../shared/actions/metadata.actions'

/*
* Main application service to initialize,
* load, set App status and initial data, and synchronize app level functionality
*/
@Injectable()
export class AppCoreService {
	/*
  	* Initialize and load metadata store structures
	*/
	constructor(private store: Store<any>) {
		console.log('AsterixDB Web Console Core Service')
		this.store.dispatch(new dataverseActions.SelectDataverses('-'));
		this.store.dispatch(new datasetActions.SelectDatasets('-'));
		this.store.dispatch(new datatypesActions.SelectDatatypes('-'));
		this.store.dispatch(new indexesActions.SelectIndexes('-'));
	}
}
