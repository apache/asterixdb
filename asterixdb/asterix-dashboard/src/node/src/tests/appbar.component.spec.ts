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

import { AppBarComponent } from "../app/dashboard/appbar.component";
import {TestBed, waitForAsync} from "@angular/core/testing";
import {StoreModule} from "@ngrx/store";
import {CUSTOM_ELEMENTS_SCHEMA} from "@angular/core";

describe('AppBarComponent', () => {
  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [
        AppBarComponent
      ],
      imports: [
        StoreModule.forRoot({})
      ],
      schemas: [
        CUSTOM_ELEMENTS_SCHEMA
      ]
    }).compileComponents();
  }));

  it('should render link titled "WEBSITE"', waitForAsync(() => {
    const fixture = TestBed.createComponent(AppBarComponent);
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('.menu:nth-child(1)').textContent).toContain('WEBSITE');
  }));

  it('should render link titled "FILE ISSUES"', waitForAsync(() => {
    const fixture = TestBed.createComponent(AppBarComponent);
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('.menu:nth-child(2)').textContent).toContain('FILE ISSUES');
  }));

  it('should render link titled "DOCUMENTATION"', waitForAsync(() => {
    const fixture = TestBed.createComponent(AppBarComponent);
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('.menu:nth-child(3)').textContent).toContain('DOCUMENTATION');
  }));

  it('should render link titled "CONTACT"', waitForAsync(() => {
    const fixture = TestBed.createComponent(AppBarComponent);
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('.menu:nth-child(4)').textContent).toContain('CONTACT');
  }));

  it('should render link titled "GITHUB"', waitForAsync(() => {
    const fixture = TestBed.createComponent(AppBarComponent);
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('.menu:nth-child(5)').textContent).toContain('GITHUB');
  }));

  it('should render link titled "METADATA"', waitForAsync(() => {
    const fixture = TestBed.createComponent(AppBarComponent);
    const compiled = fixture.debugElement.nativeElement;
    expect(compiled.querySelector('.menu:nth-child(6)').textContent).toContain('METADATA');
  }));
});
