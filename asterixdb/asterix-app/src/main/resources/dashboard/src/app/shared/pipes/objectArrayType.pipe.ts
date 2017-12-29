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
// Detecting if an object is an array
import { PipeTransform, Pipe } from '@angular/core';

@Pipe({name: 'isObjectArray'})
export class ObjectArrayTypePipe implements PipeTransform {
  transform(value, args:string[]) : any {
		return value && (value.constructor.toString().indexOf("Array") != -1)
					&& value[0] && (value[0].constructor.toString().indexOf("Object") != -1);
  }
}
