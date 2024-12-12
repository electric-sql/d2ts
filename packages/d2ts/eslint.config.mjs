import baseConfig from '../../eslint.base.mjs';

export default [
  ...baseConfig,
  {
    // Add any package-specific overrides here
    files: ['./src/**/*.ts', './tests/**/*.ts'],
    ignores: ['**/dist/**', '**/node_modules/**'],
    rules: {
      // Package-specific rules can go here
    },
  },
]; 