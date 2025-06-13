import eslint from '@eslint/js';
import tseslint from '@typescript-eslint/eslint-plugin';
import typescript from '@typescript-eslint/parser';
import prettier from 'eslint-config-prettier';

export default [
  // Base ESLint configuration
  eslint.configs.recommended,
  
  // TypeScript configurations
  {
    files: ['**/*.ts', '**/*.tsx'],
    languageOptions: {
      parser: typescript,
      parserOptions: {
        ecmaVersion: 2020,
        sourceType: 'module',
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
    },
    rules: {
      // Disable the base ESLint rule
      'no-unused-vars': 'off',
      'no-dupe-class-members': 'off',
      'no-redeclare': 'off',
      // Enable the TypeScript rule
      '@typescript-eslint/no-unused-vars': ['error', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        ignoreRestSiblings: true,
        destructuredArrayIgnorePattern: '^_'
      }],
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      'no-console': 'warn',
    },
  },

  // Prettier configuration
  prettier,

  // Global settings
  {
    languageOptions: {
      ecmaVersion: 2020,
      sourceType: 'module',
      globals: {
        // Node.js globals
        process: true,
        __dirname: true,
        __filename: true,
        exports: true,
        module: true,
        require: true,
        Buffer: true,
        console: true,
        global: true,
        globalThis: true,
        // Browser globals
        window: true,
        document: true,
        EventTarget: true,
        CustomEvent: true,
        EventListener: true,
        // Node.js types
        NodeJS: true,
        setTimeout: true,
        clearTimeout: true,
        setInterval: true,
        clearInterval: true,
        setImmediate: true,
        clearImmediate: true
      },
    },
    ignores: ['dist/', 'node_modules/'],
  },
]; 