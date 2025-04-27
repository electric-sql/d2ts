// Input is analogous to a table in a SQL database
// A Schema is a set of named Inputs
export type Input = { [key: string]: unknown }
export type Schema = { [key: string]: Input }

// Context is a Schema with a default input
export type Context<S extends Schema = Schema> = {
  schema: S
  default?: keyof S
}

// Helper types

type Flatten<T> = {
  [K in keyof T]: T[K]
} & {}

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

// Fully qualified references like "@employees.id"
type QualifiedReferencesOfSchemaString<S extends Schema> =
  RemoveIndexSignature<{
    [I in keyof S]: {
      [P in keyof S[I]]: `@${string & I}.${string & P}`
    }[keyof S[I]]
  }>

type QualifiedReferenceString<C extends Context<Schema>> =
  QualifiedReferencesOfSchemaString<
    C['schema']
  >[keyof QualifiedReferencesOfSchemaString<C['schema']>]

// Fully qualified references like { col: '@employees.id' }
type QualifiedReferencesOfSchemaObject<S extends Schema> =
  RemoveIndexSignature<{
    [I in keyof S]: {
      [P in keyof S[I]]: { col: `${string & I}.${string & P}` }
    }[keyof S[I]]
  }>

type QualifiedReferenceObject<C extends Context<Schema>> =
  QualifiedReferencesOfSchemaObject<
    C['schema']
  >[keyof QualifiedReferencesOfSchemaObject<C['schema']>]

type QualifiedReference<C extends Context<Schema>> =
  | QualifiedReferenceString<C>
  | QualifiedReferenceObject<C>

type DefaultReferencesOfSchemaString<
  S extends Schema,
  D extends keyof S,
> = RemoveIndexSignature<{
  [P in keyof S[D]]: `@${string & P}`
}>

type DefaultReferenceString<C extends Context<Schema>> =
  C['default'] extends undefined
    ? never
    : DefaultReferencesOfSchemaString<
        C['schema'],
        Exclude<C['default'], undefined>
      >[keyof DefaultReferencesOfSchemaString<
        C['schema'],
        Exclude<C['default'], undefined>
      >]

type DefaultReferencesOfSchemaObject<
  S extends Schema,
  D extends keyof S,
> = RemoveIndexSignature<{
  [P in keyof S[D]]: { col: `${string & P}` }
}>

type DefaultReferenceObject<C extends Context<Schema>> =
  C['default'] extends undefined
    ? never
    : DefaultReferencesOfSchemaObject<
        C['schema'],
        Exclude<C['default'], undefined>
      >[keyof DefaultReferencesOfSchemaObject<
        C['schema'],
        Exclude<C['default'], undefined>
      >]

type DefaultReference<C extends Context<Schema>> =
  | DefaultReferenceString<C>
  | DefaultReferenceObject<C>

type UniqueReferencesOfSchemaString<S extends Schema> = RemoveIndexSignature<{
  [I in keyof S]: {
    [P in keyof S[I]]: P extends UniquePropertyNames<S>
      ? `@${string & P}`
      : never
  }[keyof S[I]]
}>

type UniqueReferenceString<C extends Context<Schema>> =
  UniqueReferencesOfSchemaString<
    C['schema']
  >[keyof UniqueReferencesOfSchemaString<C['schema']>]

type UniqueReferencesOfSchemaObject<S extends Schema> = RemoveIndexSignature<{
  [I in keyof S]: {
    [P in keyof S[I]]: P extends UniquePropertyNames<S>
      ? { col: `${string & P}` }
      : never
  }[keyof S[I]]
}>

type UniqueReferenceObject<C extends Context<Schema>> =
  UniqueReferencesOfSchemaObject<
    C['schema']
  >[keyof UniqueReferencesOfSchemaObject<C['schema']>]

type UniqueReference<C extends Context<Schema>> =
  | UniqueReferenceString<C>
  | UniqueReferenceObject<C>

type InputWildcardString<C extends Context<Schema>> = Flatten<
  {
    [I in InputNames<C['schema']>]: `@${I}.*`
  }[InputNames<C['schema']>]
>

type InputWildcardObject<C extends Context<Schema>> = Flatten<
  {
    [I in InputNames<C['schema']>]: { col: `${I}.*` }
  }[InputNames<C['schema']>]
>

type InputWildcard<C extends Context<Schema>> =
  | InputWildcardString<C>
  | InputWildcardObject<C>

type AllWildcardString = '@*'
type AllWildcardObject = { col: '*' }
type AllWildcard = AllWildcardString | AllWildcardObject

export type PropertyReferenceString<C extends Context<Schema>> =
  | DefaultReferenceString<C>
  | QualifiedReferenceString<C>
  | UniqueReferenceString<C>

export type WildcardReferenceString<C extends Context<Schema>> =
  | InputWildcardString<C>
  | AllWildcardString

export type PropertyReferenceObject<C extends Context<Schema>> =
  | DefaultReferenceObject<C>
  | QualifiedReferenceObject<C>
  | UniqueReferenceObject<C>

export type WildcardReferenceObject<C extends Context<Schema>> =
  | InputWildcardObject<C>
  | AllWildcardObject

export type PropertyReference<C extends Context<Schema>> =
  | DefaultReference<C>
  | QualifiedReference<C>
  | UniqueReference<C>

export type WildcardReference<C extends Context<Schema>> =
  | InputWildcard<C>
  | AllWildcard

type InputWithProperty<S extends Schema, P extends string> = {
  [I in keyof RemoveIndexSignature<S>]: P extends keyof S[I] ? I : never
}[keyof RemoveIndexSignature<S>]

export type TypeFromPropertyReference<
  C extends Context<Schema>,
  R extends PropertyReference<C>,
> = R extends
  | `@${infer InputName}.${infer PropName}`
  | { col: `${infer InputName}.${infer PropName}` }
  ? InputName extends keyof C['schema']
    ? PropName extends keyof C['schema'][InputName]
      ? C['schema'][InputName][PropName]
      : never
    : never
  : R extends `@${infer PropName}` | { col: `${infer PropName}` }
    ? PropName extends keyof C['schema'][Exclude<C['default'], undefined>]
      ? C['schema'][Exclude<C['default'], undefined>][PropName]
      : C['schema'][InputWithProperty<C['schema'], PropName>][PropName]
    : never

export type InputReference<C extends Context<Schema>> = {
  [I in InputNames<C['schema']>]: I
}[InputNames<C['schema']>]

export type RenameInput<
  S extends Schema,
  I extends keyof S,
  NewName extends string,
> = Flatten<
  {
    [K in Exclude<keyof S, I>]: S[K]
  } & {
    [P in NewName]: S[I]
  }
>

export type MaybeRenameInput<
  S extends Schema,
  I extends keyof S,
  NewName extends string | undefined,
> = NewName extends undefined
  ? S
  : RenameInput<S, I, Exclude<NewName, undefined>>
