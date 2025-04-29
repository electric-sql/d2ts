import type {
  FunctionCall,
  LiteralValue,
  ExplicitLiteral,
  ConditionOperand,
  Select,
} from './schema.js'

// Input is analogous to a table in a SQL database
// A Schema is a set of named Inputs
export type Input = Record<string, unknown>
export type Schema = Record<string, Input>

// Context is a Schema with a default input
export type Context<B extends Schema = Schema, S extends Schema = Schema> = {
  baseSchema: B
  schema: S
  default?: keyof S
  result?: Record<string, unknown>
}

// Helper types

export type Flatten<T> = {
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

export type RemoveIndexSignature<T> = {
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
      [P in keyof RemoveIndexSignature<S[I]>]: `@${string & I}.${string & P}`
    }[keyof RemoveIndexSignature<S[I]>]
  }>

type QualifiedReferenceString<C extends Context<Schema>> =
  QualifiedReferencesOfSchemaString<
    C['schema']
  >[keyof QualifiedReferencesOfSchemaString<C['schema']>]

// Fully qualified references like { col: '@employees.id' }
type QualifiedReferencesOfSchemaObject<S extends Schema> =
  RemoveIndexSignature<{
    [I in keyof S]: {
      [P in keyof RemoveIndexSignature<S[I]>]: {
        col: `${string & I}.${string & P}`
      }
    }[keyof RemoveIndexSignature<S[I]>]
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

/**
 * Return the key that would be used in the result of the query for a given property
 * reference.
 * - `@id` -> `id`
 * - `@employees.id` -> `id`
 * - `{ col: 'id' }` -> `id`
 * - `{ col: 'employees.id' }` -> `id`
 */
export type ResultKeyFromPropertyReference<
  C extends Context<Schema>,
  R extends PropertyReference<C>,
> = R extends `@${infer _InputName}.${infer PropName}`
  ? PropName
  : R extends { col: `${infer _InputName}.${infer PropName}` }
    ? PropName
    : R extends `@${infer PropName}`
      ? PropName
      : R extends { col: `${infer PropName}` }
        ? PropName
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

/**
 * Helper type to combine result types from each select item in a tuple
 */
export type InferResultTypeFromSelectTuple<
  C extends Context<Schema>,
  S extends readonly Select<C>[],
> = UnionToIntersection<
  {
    [K in keyof S]: S[K] extends Select<C> ? InferResultType<C, S[K]> : never
  }[number]
>

/**
 * Convert a union type to an intersection type
 */
type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (
  x: infer I,
) => void
  ? I
  : never

/**
 * Infers the result type from a single select item
 */
type InferResultType<C extends Context<Schema>, S extends Select<C>> =
  S extends PropertyReferenceString<C>
    ? {
        [K in ResultKeyFromPropertyReference<C, S>]: TypeFromPropertyReference<
          C,
          S
        >
      }
    : S extends WildcardReferenceString<C>
      ? S extends '@*'
        ? InferAllColumnsType<C>
        : S extends `@${infer TableName}.*`
          ? TableName extends keyof C['schema']
            ? InferTableColumnsType<C, TableName>
            : {}
          : {}
      : S extends { [alias: string]: PropertyReference<C> | FunctionCall<C> }
        ? {
            [K in keyof S]: S[K] extends PropertyReference<C>
              ? TypeFromPropertyReference<C, S[K]>
              : S[K] extends FunctionCall<C>
                ? InferFunctionCallResultType<C, S[K]>
                : never
          }
        : {}

/**
 * Infers the result type for all columns from all tables
 */
type InferAllColumnsType<C extends Context<Schema>> = {
  [K in keyof C['schema']]: {
    [P in keyof C['schema'][K]]: C['schema'][K][P]
  }
}[keyof C['schema']]

/**
 * Infers the result type for all columns from a specific table
 */
type InferTableColumnsType<
  C extends Context<Schema>,
  T extends keyof C['schema'],
> = {
  [P in keyof C['schema'][T]]: C['schema'][T][P]
}

/**
 * Infers the result type for a function call
 */
type InferFunctionCallResultType<
  C extends Context<Schema>,
  F extends FunctionCall<C>,
> = F extends { SUM: any }
  ? number
  : F extends { COUNT: any }
    ? number
    : F extends { AVG: any }
      ? number
      : F extends { MIN: any }
        ? InferOperandType<C, F['MIN']>
        : F extends { MAX: any }
          ? InferOperandType<C, F['MAX']>
          : F extends { DATE: any }
            ? string
            : F extends { JSON_EXTRACT: any }
              ? unknown
              : F extends { JSON_EXTRACT_PATH: any }
                ? unknown
                : F extends { UPPER: any }
                  ? string
                  : F extends { LOWER: any }
                    ? string
                    : F extends { COALESCE: any }
                      ? InferOperandType<C, F['COALESCE']>
                      : F extends { CONCAT: any }
                        ? string
                        : F extends { LENGTH: any }
                          ? number
                          : F extends { ORDER_INDEX: any }
                            ? number
                            : unknown

/**
 * Infers the type of an operand
 */
type InferOperandType<
  C extends Context<Schema>,
  O extends ConditionOperand<C>,
> =
  O extends PropertyReference<C>
    ? TypeFromPropertyReference<C, O>
    : O extends LiteralValue
      ? O
      : O extends ExplicitLiteral
        ? O['value']
        : O extends FunctionCall<C>
          ? InferFunctionCallResultType<C, O>
          : O extends ConditionOperand<C>[]
            ? InferOperandType<C, O[number]>
            : unknown
