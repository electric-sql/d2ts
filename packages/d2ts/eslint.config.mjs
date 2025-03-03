import baseConfig from '../../eslint.base.mjs';

export default [
  ...baseConfig,
  {
    files: ['**/*.ts'],
    ignores: ['**/dist/**', '**/node_modules/**'],
    rules: {
      // Package-specific rules can go here
    },
  },
  {
    files: ['**/tests/**/*.ts'],
    rules: {
      '@typescript-eslint/no-unused-vars': 'off',
    },
  },
]; 