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
import { Component, Inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import * as dataverseActions from '../../shared/actions/dataverse.actions';
import * as datasetActions from '../../shared/actions/dataset.actions';
import * as datatypesActions from '../../shared/actions/datatype.actions';
import * as indexesActions from '../../shared/actions/index.actions';
import * as functionActions from '../../shared/actions/function.actions';
import { ViewChild} from '@angular/core';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import {MatTreeNestedDataSource} from "@angular/material/tree";
import {NestedTreeControl} from "@angular/cdk/tree";
import {Data} from "@angular/router";

interface DataTypeNode {
  DatatypeName: string;
  DatatypeType?: string;
  fields?: DataTypeNode[];
  primitive: boolean;
  isNullable?: boolean;
  isMissable?: boolean;
  OrderedList?: boolean;
  UnorderedList?: boolean;
  anonymous: boolean;
}

enum MetadataTypes {
  record = "RECORD",
  orderedList = "ORDEREDLIST",
  unorderedList = "UNORDEREDLIST"
}

enum MetadataTypesNames {
  record = "Record",
  orderedList = "Ordered List",
  unorderedList = "Unordered List"
}

enum MetadataListTypes {
  orderedList = "OrderedList",
  unorderedList = "UnorderedList"
}

@Component({
    moduleId: module.id,
    selector: 'awc-metadata',
    templateUrl: 'metadata.component.html',
    styleUrls: ['metadata.component.scss']
})

export class MetadataComponent {
    dataverses$: Observable<any>;
    dataverses: any;
    datasetsFiltered: any;
    datasets$: Observable<any>;
    datasets: any;
    datatypesFiltered :any;
    datatypes$: Observable<any>;
    datatypes: any;
    indexesFiltered: any;
    indexes$: Observable<any>;
    indexes: any;
    functionsFiltered: any;
    functions$: Observable<any>;
    functions: any;
    curr_dialogRef: any;
    dialogActive: boolean;

    //added variables for sample
    sampleDataset$: Observable<any>;
    sampleDataset: any;

    checkedDataverses: any = {};

    //added variables for flattening
    datatypesDict: Object;

    dialogSamples = {};

    constructor(private store: Store<any>, public dialog: MatDialog) {
        this.refreshMetadata();
        this.dialogActive = false;
    }

    ngOnInit() {
        this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);

         // Watching for Dataverses
         this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);
         this.dataverses$.subscribe((data: any[]) => {
             this.dataverses = data;

             if (Object.keys(this.checkedDataverses).length > 0) {
               for (let dataverse of this.dataverses) {
                 if (this.checkedDataverses[dataverse.DataverseName]) {
                   dataverse.active = true;
                 }
               }

               this.checkedDataverses = {};
             }
         });

         // Watching for Datasets
         this.datasets$ = this.store.select(s => s.dataset.datasets.results);
         this.datasets$.subscribe((data: any[]) => {
                this.datasets = data;
                this.datasetsFiltered = this.filter(this.datasets);
         });

         // Watching for Datatypes
         this.datatypes$ = this.store.select(s => s.datatype.datatypes.results);
         this.datatypes$.subscribe((data: any[]) => {
            this.datatypes = data;
            this.datatypesFiltered = this.filter(this.datatypes);
            this.datatypesDict = this.createDatatypeDict(this.datatypes);
         });

         // Watching for indexes
         this.indexes$ = this.store.select(s => s.index.indexes.results);
         this.indexes$.subscribe((data: any[]) => {
            this.indexes = data;
            this.indexesFiltered = this.filter(this.indexes);
         });

         //Watching for functions
        this.functions$ = this.store.select(s => s.functions.functions.results);
        this.functions$.subscribe((data: any[]) => {
          this.functions = data;
          this.functionsFiltered = this.filter(this.functions);
        });

         // Watching for samples
        this.sampleDataset$ = this.store.select(s => s.dataset.sample);
    }

    refreshMetadata() {
        if (this.dataverses) {
          for (let dataverse of this.dataverses) {
            if (dataverse.active) {
              this.checkedDataverses[dataverse.DataverseName] = true;
            }
          }
        }

        this.store.dispatch(new dataverseActions.SelectDataverses('-'));
        this.store.dispatch(new datasetActions.SelectDatasets('-'));
        this.store.dispatch(new datatypesActions.SelectDatatypes('-'));
        this.store.dispatch(new indexesActions.SelectIndexes('-'));
        this.store.dispatch(new functionActions.SelectFunctions('-'));
    }

    dataverseFilter = {}
    dataverseFilterMap = new Map();

    createDatatypeDict(data) {
        let newDict = new Object();

        if (data) {
            for (let i=0; i < data.length; i++) {
                newDict[data[i].DataverseName + "." + data[i].DatatypeName] = i;
            }
        }

        return newDict;
    }

    filter(data){
        let results = [];

        if (data) {
            for (let i=0; i< data.length; i++){
                let keyCompare = data[i].DataverseName
                this.dataverseFilterMap.forEach((value: boolean, key: string) => {
                    if (keyCompare === key) {
                        results.push(data[i]);
                    }
                });
            }
        }
        return results;
    }

    @ViewChild('datasetsPanel') datasetsPanel;
    panelOpenState: boolean = false;

    checkStatus = [];
    generateFilter(dataverse, event, i) {
        if (this.checkStatus[i] == undefined) {
            this.checkStatus.push(event.checked);
        } else {
            this.checkStatus[i] = event.checked;
        }

        if (event.checked === true) {
            this.dataverseFilter[dataverse] = event.checked;
            this.dataverseFilterMap.set(dataverse, event.checked);
        } else {
            delete this.dataverseFilter[dataverse];
            this.dataverseFilterMap.delete(dataverse);
        }

        this.datasetsFiltered = this.filter(this.datasets);
        this.datatypesFiltered = this.filter(this.datatypes);
        this.indexesFiltered = this.filter(this.indexes);
        this.functionsFiltered = this.filter(this.functions);

        /* Open the dataset expansion panel if there is anything to show */
        if (this.datasetsFiltered.length > 0) {
            this.panelOpenState = true;
        } else {
            this.panelOpenState = false;
        }
    }

    /*
    * Traverse Metadata recursively, handles Metadata dataverse as well as regular dataverses
     */
    recursiveMetadataTraverse(data, toCreate): Object {
      toCreate.DatatypeName = data.DatatypeName;
      //primitive == no Derived field or Derived == undefined
      if (data.Derived == undefined) {
        //if primitive
        toCreate.DatatypeName = data.DatatypeName;
        toCreate.fields = [];
        toCreate.primitive = true;
      } else {
        //if not primitive, need to check .Derived exists every time, or else handle primitive type
        toCreate.DatatypeType = data.Derived.Tag;

        //determine what type it is (Record, Ordered List or Unordered List). Ordered list and unordered list are handled the same
        let list_type = "";

        switch(toCreate.DatatypeType) {
          case MetadataTypes.record:
            toCreate.DatatypeType = MetadataTypesNames.record;
            break;
          case MetadataTypes.orderedList:
            toCreate.DatatypeType = MetadataTypesNames.orderedList;
            list_type = MetadataListTypes.orderedList;
            break;
          case MetadataTypes.unorderedList:
            toCreate.DatatypeType = MetadataTypesNames.unorderedList;
            list_type = MetadataListTypes.unorderedList;
            break;
          default:
            break;
        }

        toCreate.fields = [];

        if (data.Derived.Tag == "RECORD") {
          // if it is a record, we must iterate over the fields and may have to recurse if there is a non primitive type
          for (let field of data.Derived.Record.Fields) {
            //if it is NOT a primitive type
            if ((data.DataverseName + "." + field.FieldType) in this.datatypesDict &&
              this.datatypes[this.datatypesDict[data.DataverseName + "." + field.FieldType]].Derived != undefined) {
              field.Nested = true;

              //get the nested object from datatypesDict
              let nestedName = this.datatypesDict[data.DataverseName + "." + field.FieldType];
              let nestedObject = this.datatypes[nestedName];

              let nested_field = {
                DatatypeName: field.FieldName,
                DatatypeType: field.FieldType,
                primitive: false,
                fields: [],
                isNullable: field.IsNullable,
                isMissable: field.IsMissable,
                OrderedList: false,
                UnorderedList: false,
                anonymous: nestedObject.Derived.IsAnonymous,
              }

              if (nestedObject.Derived.Tag == "RECORD") {
                //object and should iterate over fields
                field.NestedType = "Record";
                field.NestedTypeType = nestedObject.DatatypeName;

                let recurse_result = this.recursiveMetadataTraverse(nestedObject, {})

                field.NestedRecord = recurse_result[0];

                let toAdd = recurse_result[1];
                toAdd.DatatypeType = "Record";
                toAdd.primitive = false;
                toAdd.anonymous = nestedObject.Derived.IsAnonymous;

                nested_field.fields.push(toAdd);
              }
              else {
                let listObject;
                let nestedListType = "";
                let nestedListTypeName = "";

                //determine the type of list of the nested object
                if (nestedObject.Derived.Tag == MetadataTypes.orderedList) {
                  nestedListType = MetadataListTypes.orderedList;
                  nestedListTypeName = MetadataTypesNames.orderedList;
                } else {
                  nestedListType = MetadataListTypes.unorderedList;
                  nestedListTypeName = MetadataTypesNames.unorderedList;
                }

                nested_field[nestedListType] = true;
                field.NestedType = nestedListTypeName;

                if (data.DataverseName + "." + nestedObject.Derived[nestedListType] in this.datatypesDict) {
                  field.primitive = false;
                  listObject = this.datatypes[this.datatypesDict[data.DataverseName + "." + nestedObject.Derived[nestedListType]]];

                  let recurse_result = this.recursiveMetadataTraverse(listObject, {});

                  field.NestedRecord = recurse_result[0];

                  let toAdd = recurse_result[1];

                  if (toAdd.DatatypeType == nestedListTypeName) {
                    toAdd[nestedListType] = true;
                  } else {
                    toAdd[nestedListType] = false;
                  }

                  toAdd.primitive = false;
                  if (listObject.Derived != undefined)
                    toAdd.anonymous = listObject.Derived.IsAnonymous;

                  nested_field.fields.push(toAdd);
                } else {
                  field.primitive = true;
                  nested_field.fields.push({
                    DatatypeName: nestedObject.Derived[nestedListType],
                    [nestedListType]: true,
                    primitive: true,
                  });
                }
              }

              toCreate.fields.push(nested_field);
            }
            else {
              field.Nested = false;
              toCreate.fields.push({
                DatatypeName: field.FieldName,
                DatatypeType: field.FieldType,
                primitive: true,
                isMissable: field.IsMissable,
                isNullable: field.IsNullable,
                anonymous: false,
              });
            }
          }
        } else {
          let listItem = this.datatypes[this.datatypesDict[data.DataverseName + "." + data.Derived[list_type]]];

          toCreate[list_type] = true;

          if (listItem == undefined) {
            toCreate.fields.push({
              DatatypeName: data.Derived[list_type],
              [list_type]: true,
              primitive: true,
              anonymous: data.Derived.IsAnonymous,
            })
          } else {
            let recurse_result = this.recursiveMetadataTraverse(listItem, {});

            let toAdd = recurse_result[1];
            toAdd.primitive = false;
            if (listItem.Derived != undefined)
              toAdd.anonymous = listItem.Derived.IsAnonymous;
            toCreate.fields.push(toAdd);
          }
        }
      }

      return [data, toCreate];
    }

    /*
    * opens the metadata inspector
    */
    openMetadataInspectorDialog(data, metadata_type): void {
        let metadata = JSON.stringify(data, null, 8);
        metadata = metadata.replace(/^{/, '');
        metadata = metadata.replace(/^\n/, '');
        metadata = metadata.replace(/}$/, '');

        //if metadata_type is dataset, sample said dataset and add to data to be displayed.

        data.guts = metadata;

        data.MetadataType = metadata_type
        if (metadata_type == 'dataset') {
            let dataset = "`" + data.DataverseName + "`" + "." + "`" + data.DatasetName + "`";
            this.store.dispatch(new datasetActions.SampleDataset({dataset: dataset}));

            this.sampleDataset$.subscribe((resp: any[]) => {
                if (resp) {
                  this.dialogSamples[dataset] = JSON.stringify(resp[dataset], null, 2);
                  data.sample = this.dialogSamples[dataset];
                } else {
                  data.sample = undefined;
                }
            });
        }

        //flatten data if datatype
        if (metadata_type == 'datatype') {
            let new_datatype = new Object();

            let recurseResults = this.recursiveMetadataTraverse(data, new_datatype);
            let converted = recurseResults[1];

            data = recurseResults[0];

            data.DataTypeTree = converted;
        }

        this.curr_dialogRef = this.dialog.open(DialogMetadataInspector, {
            minWidth: '500px',
            data: data,
            hasBackdrop: false
        });
    }
}

@Component({
    selector: 'dataset-create-dialog',
    templateUrl: 'metadata-inspector.component.html',
    styleUrls: ['metadata-inspector.component.scss']
})

export class DialogMetadataInspector {
    constructor(  public dialogCreateDsRef: MatDialogRef<DialogMetadataInspector>,
                  @Inject(MAT_DIALOG_DATA) public data: any) {
        if (data.MetadataType == "datatype") {
            this.dataSource.data = data.DataTypeTree.fields;
        }
    }

    onClickClose() {
        this.dialogCreateDsRef.close(this.data['dialogID']);
    }

    onClickJSON() {
        this.showGuts = true;
        this.hideJSONButton = true;
    }

    onClickParsed() {
        this.showGuts = false;
        this.hideJSONButton = false;
    }

    showGuts = false;
    hideJSONButton = false

    treeControl = new NestedTreeControl<DataTypeNode>(node => node.fields);
    dataSource = new MatTreeNestedDataSource<DataTypeNode>();
    hasChild = (_: number, node: DataTypeNode) => !!node.fields && node.fields.length > 0;
}
