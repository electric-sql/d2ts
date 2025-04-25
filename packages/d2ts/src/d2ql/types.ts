// Input is analogous to a table in a SQL database
// A Schema is a set of named Inputs
export type Input = { [key: string]: unknown }
export type Schema = { [key: string]: Input }

// Context is a Schema with a default input
export type Context<S extends Schema> = {
  schema: S
  default: keyof S
}

// Helper types

type Flatten<T> = {
  [K in keyof T]: T[K];
} & {};

type UniqueSecondLevelKeys<T> = {
  [K in keyof T]: Exclude<
    keyof T[K],
    // all keys in every branch except K
    {
      [P in Exclude<keyof T, K>]: keyof T[P]
    }[Exclude<keyof T, K>]
  >
}[keyof T]

type InputNames<S extends Schema> = RemoveIndexSignature<{
  [I in keyof S]: I
}>[keyof RemoveIndexSignature<{
  [I in keyof S]: I
}>]

type InputPropertyNames<
  S extends Schema,
  InputName extends InputNames<S>,
> = InputName extends any ? keyof S[InputName] : never

type PropertyNamesByInput<S extends Schema> = {
  [I in InputNames<S>]: InputPropertyNames<S, I>
}

type AllPropertyNames<S extends Schema> =
  PropertyNamesByInput<S>[InputNames<S>]

type UniquePropertyNames<S extends Schema> = UniqueSecondLevelKeys<
  RemoveIndexSignature<S>
>

type RemoveIndexSignature<T> = {
  [K in keyof T as string extends K
    ? never
    : number extends K
      ? never
      : K]: T[K]
}

// Fully qualified references like @employees.id
type QualifiedReferencesOfSchema<S extends Schema> = RemoveIndexSignature<{
  [I in keyof S]: {
    [P in keyof S[I]]: `@${string & I}.${string & P}`
  }[keyof S[I]]
}>

type QualifiedReferenceOfSchema<S extends Schema> =
  QualifiedReferencesOfSchema<S>[keyof QualifiedReferencesOfSchema<S>]

type QualifiedReference<C extends Context<Schema>> = QualifiedReferenceOfSchema<
  C['schema']
>

type DefaultReferencesOfSchema<
  S extends Schema,
  D extends keyof S,
> = RemoveIndexSignature<{
  [P in keyof S[D]]: `@${string & P}`
}>

type DefaultReferenceOfSchema<
  S extends Schema,
  D extends keyof S,
> = DefaultReferencesOfSchema<S, D>[keyof DefaultReferencesOfSchema<S, D>]

type DefaultReference<C extends Context<Schema>> = DefaultReferenceOfSchema<
  C['schema'],
  C['default']
>

type UniqueReferencesOfSchema<S extends Schema> = RemoveIndexSignature<{
  [I in keyof S]: {
    [P in keyof S[I]]: P extends UniquePropertyNames<S>
      ? `@${string & P}`
      : never
  }[keyof S[I]]
}>

type UniqueReference<C extends Context<Schema>> = UniqueReferencesOfSchema<
  C['schema']
>[keyof UniqueReferencesOfSchema<C['schema']>]

type InputWildcard<C extends Context<Schema>> = Flatten<{
  [I in InputNames<C['schema']>]: `@${I}.*`
}[InputNames<C['schema']>]>

type AllWildcard = '@*'

export type PropertyReference<C extends Context<Schema>> =
  | DefaultReference<C>
  | QualifiedReference<C>
  | UniqueReference<C>

export type WildcardReference<C extends Context<Schema>> =
  | InputWildcard<C>
  | AllWildcard

type InputWithProperty<S extends Schema, P extends string> = {
  [I in keyof RemoveIndexSignature<S>]: P extends keyof S[I] ? I : never
}[keyof RemoveIndexSignature<S>];

export type TypeFromPropertyReference<C extends Context<Schema>, R extends PropertyReference<C>> =
  R extends `@${infer InputName}.${infer PropName}`
    ? InputName extends keyof C['schema']
      ? PropName extends keyof C['schema'][InputName]
        ? C['schema'][InputName][PropName]
        : never
      : never
    : R extends `@${infer PropName}`
      ? PropName extends keyof C['schema'][C['default']]
        ? C['schema'][C['default']][PropName] 
        : C['schema'][InputWithProperty<C['schema'], PropName>][PropName]
      : never;

interface TextSchema extends Schema {
  employees: {
    id: number
    name: string
    email: string
  }
  departments: {
    id: number
    name: string
    location: string
    something: number
  }
  somethingElse: {
    id: number
    something: number
  }
}

type t = TypeFromPropertyReference<{
  schema: TextSchema
  default: 'employees'
}, '@location'>

type test1 = InputPropertyNames<TextSchema, 'employees' | 'departments'>
type test2 = InputPropertyNames<TextSchema, 'employees'>
type test3 = PropertyNamesByInput<TextSchema>[InputNames<TextSchema>]

type qualified = QualifiedReference<{
  schema: TextSchema
  default: 'employees'
}>

type defaulted = DefaultReference<{
  schema: TextSchema
  default: 'employees'
}>

type unique = UniqueReference<{
  schema: TextSchema
  default: 'employees'
}>

type wildcard = WildcardReference<{
  schema: TextSchema
  default: 'employees'
}>

type All = PropertyReference<{
  schema: TextSchema
  default: 'employees'
}>
// All of these should be valid
// @employees.id
// @employees.name
// @employees.email
// @departments.id
// @departments.name
// @departments.location
// @departments.something
// @somethingElse.id
// @somethingElse.something
// Valid because they are the default input
// @id
// @name
// @email
// Valid because there is only one input with a name on all the tables in the context
// @location
