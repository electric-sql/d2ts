import { describe, it } from 'vitest';
import { expectTypeOf } from 'vitest';
import {
  Input,
  Schema,
  Context,
  PropertyReference,
  WildcardReference,
  TypeFromPropertyReference
} from '../../src/d2ql/types';

// Define a test schema
interface TestSchema extends Schema {
  users: {
    id: number;
    name: string;
    email: string;
  };
  posts: {
    id: number;
    title: string;
    content: string;
    authorId: number;
    views: number;
  };
  comments: {
    id: number;
    postId: number;
    userId: number;
    content: string;
  };
}

// Test context with users as default
interface UsersContext extends Context<TestSchema> {
  schema: TestSchema;
  default: 'users';
}

describe('d2ql types', () => {
  describe('Input type', () => {
    it('should handle basic input objects', () => {
      expectTypeOf<Input>().toBeObject();
      expectTypeOf<TestSchema['users']>().toMatchTypeOf<Input>();
    });
  });

  describe('Schema type', () => {
    it('should be a collection of inputs', () => {
      expectTypeOf<Schema>().toBeObject();
      expectTypeOf<TestSchema>().toMatchTypeOf<Schema>();
      expectTypeOf<TestSchema>().toHaveProperty('users');
      expectTypeOf<TestSchema>().toHaveProperty('posts');
      expectTypeOf<TestSchema>().toHaveProperty('comments');
    });
  });

  describe('Context type', () => {
    it('should have schema and default properties', () => {
      expectTypeOf<Context<TestSchema>>().toBeObject();
      expectTypeOf<Context<TestSchema>>().toHaveProperty('schema');
      expectTypeOf<Context<TestSchema>>().toHaveProperty('default');
      expectTypeOf<UsersContext['default']>().toEqualTypeOf<'users'>();
    });
  });

  describe('PropertyReference type', () => {
    it('should accept qualified references', () => {
      expectTypeOf<'@users.id'>().toMatchTypeOf<PropertyReference<UsersContext>>();
      expectTypeOf<'@posts.authorId'>().toMatchTypeOf<PropertyReference<UsersContext>>();
    });

    it('should accept default references', () => {
      expectTypeOf<'@id'>().toMatchTypeOf<PropertyReference<UsersContext>>();
      expectTypeOf<'@name'>().toMatchTypeOf<PropertyReference<UsersContext>>();
    });

    it('should accept unique references', () => {
      // 'views' only exists in posts
      expectTypeOf<'@views'>().toMatchTypeOf<PropertyReference<UsersContext>>();
      // 'content' exists in both posts and comments, so not a unique reference
      // This should fail type checking if uncommented:
      // expectTypeOf<'@content'>().toMatchTypeOf<PropertyReference<UsersContext>>();
    });
  });

  describe('WildcardReference type', () => {
    it('should accept input wildcards', () => {
      expectTypeOf<'@users.*'>().toMatchTypeOf<WildcardReference<UsersContext>>();
      expectTypeOf<'@posts.*'>().toMatchTypeOf<WildcardReference<UsersContext>>();
    });

    it('should accept global wildcard', () => {
      expectTypeOf<'@*'>().toMatchTypeOf<WildcardReference<UsersContext>>();
    });
  });

  describe('TypeFromPropertyReference type', () => {
    it('should resolve qualified references', () => {
      expectTypeOf<
        TypeFromPropertyReference<UsersContext, '@users.id'>
      >().toEqualTypeOf<number>();
      
      expectTypeOf<
        TypeFromPropertyReference<UsersContext, '@posts.title'>
      >().toEqualTypeOf<string>();
    });

    it('should resolve default references', () => {
      expectTypeOf<
        TypeFromPropertyReference<UsersContext, '@id'>
      >().toEqualTypeOf<number>();
      
      expectTypeOf<
        TypeFromPropertyReference<UsersContext, '@name'>
      >().toEqualTypeOf<string>();
    });

    it('should resolve unique references', () => {
      // 'views' only exists in posts
      expectTypeOf<
        TypeFromPropertyReference<UsersContext, '@views'>
      >().toEqualTypeOf<number>();
      
      // 'authorId' only exists in posts
      expectTypeOf<
        TypeFromPropertyReference<UsersContext, '@authorId'>
      >().toEqualTypeOf<number>();
    });
  });
}); 