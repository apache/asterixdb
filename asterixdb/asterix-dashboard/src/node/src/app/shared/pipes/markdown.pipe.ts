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
import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';
import DOMPurify from 'dompurify';

/*
* Renders Markdown (the assistant's natural-language output) to sanitized HTML.
* The LLM response is untrusted, so the generated HTML is always passed through
* DOMPurify before it is trusted for binding — never bypass the sanitizer on raw
* model output.
*/
@Pipe({
    name: 'markdown',
    standalone: false
})
export class MarkdownPipe implements PipeTransform {
    constructor(private sanitizer: DomSanitizer) {}

    transform(value: string | null | undefined): SafeHtml {
        if (!value) {
            return '';
        }
        const rendered = marked.parse(value, { gfm: true, breaks: true, async: false }) as string;
        const clean = DOMPurify.sanitize(rendered, { ADD_ATTR: ['target', 'rel'] });
        return this.sanitizer.bypassSecurityTrustHtml(clean);
    }
}
