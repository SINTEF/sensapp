import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import ini from 'highlight.js/lib/languages/ini';
import python from 'highlight.js/lib/languages/python';
import yaml from 'highlight.js/lib/languages/yaml';

hljs.registerLanguage('python', python);
hljs.registerLanguage('bash', bash);
hljs.registerLanguage('ini', ini);
hljs.registerLanguage('yaml', yaml);

/** The names highlight.js knows the languages of the page by (it reads TOML as ini). */
const NAMES = { python: 'python', bash: 'bash', toml: 'ini', yaml: 'yaml' } as const;

/** The code as HTML with its colors. highlight.js escapes the text it is given: the result is safe to put in the page. */
export function highlight(code: string, language: keyof typeof NAMES): string {
  return hljs.highlight(code, { language: NAMES[language] }).value;
}
