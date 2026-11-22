import { defineParadoxConfig } from '@ankhorage/paradox';

export default defineParadoxConfig({
  mode: 'write',
  docs: {
    title: '@ankhorage/graph-neptune',
    description: 'Amazon Neptune Database Bolt persistence adapter for @ankhorage/graph.',
  },
  package: {
    root: '.',
    entrypoints: ['src/neptune.ts'],
  },
  output: { dir: './paradox' },
});
