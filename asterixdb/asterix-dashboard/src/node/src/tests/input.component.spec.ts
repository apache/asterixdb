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
import {InputQueryComponent} from "../app/dashboard/query/input.component";
import {ComponentFixture, TestBed, waitForAsync, async} from "@angular/core/testing";
import {provideMockStore, MockStore} from "@ngrx/store/testing";
import {CUSTOM_ELEMENTS_SCHEMA, DebugElement} from "@angular/core";
import {By} from "@angular/platform-browser";
import {HarnessLoader} from "@angular/cdk/testing";
import {TestbedHarnessEnvironment} from "@angular/cdk/testing/testbed";

describe('InputQueryComponent: unit test', () => {
  let component: InputQueryComponent;
  let fixture: ComponentFixture<InputQueryComponent>;
  let de: DebugElement;
  let el: HTMLElement;
  let loader: HarnessLoader;

  let store: MockStore;
  const initialState = {
    sqlQuery: {
      currentRequestId: '',
      loadingHash: {},
      loadedHash: {},
      successHash:{},
      errorHash: {},
      sqlQueryString: "",
      sqlQueryPlanFormat: "",
      sqlQueryOutputFormat: "",
      sqlQueryStringHash: {},
      sqlQueryPlanFormatHash: {},
      sqlQueryResultHash: {},
      sqlQueryErrorHash: {},
      sqlQueryPrepared: {},
      sqlQueryMetrics: {},
      sqlQueryWarnings: {}
    },
    cancelQuery: {
      currentRequestId: "",
      success: false
    },
    dataverse: {
      loaded: false,
      loading: false,
      dataverses: [],
      createDataverse: [],
      createDataverseName: "",
      createDataverseError: [],
      createDataverseSuccess: false,
      createDataverseFailed: false,
      dropDataverse: [],
      dropDataverseName: "",
      dropDataverseError: [],
      dropDataverseSuccess: false,
      dropDataverseFailed: false,
      defaultDataverseName: 'Default'
    }
  }

  beforeEach(async() => {
    await TestBed.configureTestingModule({
      declarations: [
        InputQueryComponent
      ],
      providers: [
        provideMockStore({initialState})
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    store = TestBed.inject(MockStore);
    spyOn(store, 'dispatch');
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 30000;
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InputQueryComponent);
    component = fixture.componentInstance;
    de = fixture.debugElement;
    fixture.detectChanges();
    loader = TestbedHarnessEnvironment.loader(fixture);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  })

  //query navigation
  describe('query navigation tests', () => {
    it('running query should call store.dispatch 3 times', () => {
      expect(store.dispatch).toHaveBeenCalledTimes(3);

      component.queryString = "select 1";
      component.onClickRun();
      fixture.detectChanges();
      expect(store.dispatch).toHaveBeenCalledTimes(6);

      component.queryString = "select 2";
      component.onClickRun();
      fixture.detectChanges();
      expect(store.dispatch).toHaveBeenCalledTimes(9);

      component.queryString = "select 3";
      component.onClickRun();
      fixture.detectChanges();
      expect(store.dispatch).toHaveBeenCalledTimes(12);
    });
  });

  //outputs
  describe('test outputs', () => {
    it('emits isExplain is false on click run', () => {
      let actualInputToOutput = {};

      component.inputToOutputEmitter.subscribe((inputToOutput) => {
        actualInputToOutput = inputToOutput;
      })

      let button = de.query(By.css('.run'));
      button.nativeElement.click();

      expect(actualInputToOutput['isExplain']).toBe(false);
    });

    it('emits isExplain is true on click explain', () => {
      let actualInputToOutput: boolean = undefined;

      component.inputToOutputEmitter.subscribe((inputToOutput) => {
        actualInputToOutput = inputToOutput['isExplain'];
      })

      let button = de.query(By.css('.explain'));
      button.nativeElement.click();

      expect(actualInputToOutput).toBe(true);
    });

    it('emits JSON output format when selected from dropdown', async()=> {
      let outputFormat: string = undefined;

      component.inputToOutputEmitter.subscribe((inputToOutput) => {
        outputFormat = inputToOutput['outputFormat'];
      })

      component.outputOptions = 'JSON';

      let button = de.query(By.css('.run'));
      button.nativeElement.click();

      expect(outputFormat).toEqual('JSON');
    });

    it('emits CSV (no header) output format when selected from dropdown', async() => {
      let outputFormat: string = undefined;

      component.inputToOutputEmitter.subscribe((inputToOutput) => {
        outputFormat = inputToOutput['outputFormat'];
      })

      component.outputOptions = 'CSV';

      let button = de.query(By.css('.run'));
      button.nativeElement.click();

      expect(outputFormat).toEqual('CSV');
    });

    it('emits CSV (header) output format when selected from dropdown', () => {
      let outputFormat: string = undefined;


      component.inputToOutputEmitter.subscribe((inputToOutput) => {
        outputFormat = inputToOutput['outputFormat'];
      })

      component.outputOptions = 'CSV_header'

      let button = de.query(By.css('.run'));
      button.nativeElement.click();

      expect(outputFormat).toEqual('CSV_header');
    });
  });

  //clear tests
  describe('clear() tests', () => {
    it('clicking #clear() should clear query string', () => {
      component.queryString = "select 1";
      expect(component.queryString).toBe("select 1");
      component.onClickClear();
      expect(component.queryString).toBe("");
    });

    it('clicking clear should clear the editor on screen', async() => {
      component.editor.getDoc().setValue("this is a test text");

      let button = de.query(By.css('.clear'));
      button.nativeElement.click();

      expect(component.editor.getDoc().getValue()).toEqual("");
      expect(component.queryString).toEqual("");
    });

  });
});
