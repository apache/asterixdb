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
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule } from '@angular/common/http';
import { EffectsModule } from '@ngrx/effects';
import { DataverseEffects } from './shared/effects/dataverse.effects';
import { DatasetEffects } from './shared/effects/dataset.effects';
import { DatatypeEffects } from './shared/effects/datatype.effects';
import { IndexEffects } from './shared/effects/index.effects';
import { SQLQueryEffects } from './shared/effects/query.effects';
import { MetadataEffects } from './shared/effects/metadata.effects';
import { AppComponent } from './app.component';
import { AppBarComponent }  from './dashboard/appbar.component';
import { DataverseCollection } from './dashboard/metadata/dataverses-collection/dataverses.component';
import { DatasetCollection } from './dashboard/metadata/datasets-collection/datasets.component';
import { DatatypeCollection } from './dashboard/metadata/datatypes-collection/datatypes.component';
import { CodemirrorComponent } from './dashboard/query/codemirror.component';
import { CodemirrorMetadataComponent } from './dashboard/metadata/codemirror-metadata.component';
import { IndexCollection } from './dashboard/metadata/indexes-collection/indexes.component';
import { MetadataContainerComponent }  from './dashboard/metadata/metadata-container.component';
import { MetadataComponent }  from './dashboard/query/metadata.component';
import { QueryContainerComponent }  from './dashboard/query/query-container.component';
import { InputQueryComponent }  from './dashboard/query/input.component';
import { InputQueryMetadataComponent }  from './dashboard/metadata/input-metadata.component';
import { QueryOutputComponent, SafeHtmlPipe }  from './dashboard/query/ouput.component';
import { AppTabComponent }  from './dashboard/apptab.component';
import { KeysPipe } from './shared/pipes/keys.pipe';
import { ObjectTypePipe } from './shared/pipes/objectType.pipe';
import { ObjectArrayTypePipe } from './shared/pipes/objectArrayType.pipe';
import { reducers } from './shared/reducers';
import { SQLService } from './shared/services/async-query.service'
import { AppCoreService } from './shared/services/app-core.service'
import { MetadataService } from './shared/services/async-metadata.service'
import { DBModule } from '@ngrx/db';
import { FormsModule } from '@angular/forms';
import { MaterialModule } from './material.module';
import { NgModule } from '@angular/core';
import { StoreModule,  } from '@ngrx/store';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { schema } from './db';
import { DataTableModule, SharedModule } from 'primeng/primeng';
import { TreeModule, TreeNode} from 'primeng/primeng';
import { DialogCreateDataverse, DialogDropDataverse } from './dashboard/metadata/dataverses-collection/dataverses.component';
import { DialogCreateDataset, DialogDropDataset } from './dashboard/metadata/datasets-collection/datasets.component';
import { DialogCreateDatatype, DialogDropDatatype } from './dashboard/metadata/datatypes-collection/datatypes.component';
import { DialogCreateIndex, DialogDropIndex } from './dashboard/metadata/indexes-collection/indexes.component';



@NgModule({
  declarations: [
    AppComponent,
    AppBarComponent,
    InputQueryComponent,
    InputQueryMetadataComponent,
		QueryOutputComponent,
    CodemirrorComponent,
    CodemirrorMetadataComponent,
		DataverseCollection,
		DatasetCollection,
		DatatypeCollection,
		IndexCollection,
    KeysPipe,
		MetadataContainerComponent,
    MetadataComponent,
    QueryContainerComponent,
		AppTabComponent,
		ObjectTypePipe,
    ObjectArrayTypePipe,
    DialogCreateDataverse,
    DialogDropDataverse,
    DialogCreateDataset,
    DialogDropDataset,
    DialogCreateDatatype,
    DialogDropDatatype,
    DialogCreateIndex,
    DialogDropIndex,
    SafeHtmlPipe
  ],
  imports: [
    TreeModule,
    DataTableModule,
    SharedModule,
    FormsModule,
    BrowserModule,
		BrowserAnimationsModule,
		DBModule.provideDB(schema),
		EffectsModule.forRoot([MetadataEffects, DataverseEffects, DatasetEffects, DatatypeEffects, IndexEffects, SQLQueryEffects]),
    HttpClientModule,
    MaterialModule,
		StoreModule.forRoot(reducers),
		StoreDevtoolsModule.instrument({
			maxAge: 10
		})
  ],
  entryComponents: [
    DialogCreateDataverse, 
    DialogDropDataverse, 
    DialogCreateDataset, 
    DialogDropDataset , 
    DialogCreateDatatype, 
    DialogDropDatatype,
    DialogCreateIndex, 
    DialogDropIndex 
  ],
  providers: [AppCoreService, SQLService, MetadataService],
  bootstrap: [AppComponent]
})
export class AppModule { }
