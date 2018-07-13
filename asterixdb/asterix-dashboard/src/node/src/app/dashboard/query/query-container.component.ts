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
import { Component, AfterViewInit} from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';

/*
 * query component
 * has editor (codemirror) for writing some query
 */
@Component({
    moduleId: module.id,
    selector: 'awc-query-container',
    templateUrl:'query-container.component.html',
    styleUrls: ['query-container.component.scss']
})

export class QueryContainerComponent implements AfterViewInit {
    sideMenuVisible$: Observable<any>;
    visible = false;

    constructor(private store: Store<any>) {}

    ngAfterViewInit() {
        this.sideMenuVisible$ = this.store.select(s => s.app.sideMenuVisible);
        this.sideMenuVisible$.subscribe((data: any) => {
            if (data === true) {
                this.visible = true;
            } else {
                this.visible = false;
            }
        })
    }
}