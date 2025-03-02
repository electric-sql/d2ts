import { AllowedFunctionName, ConditionOperand } from './schema.js'

/**
 * Type for function implementations
 */
type FunctionImplementation = (arg: unknown) => unknown

/**
 * Map of function names to their implementations
 */
const functionImplementations: Record<
  AllowedFunctionName,
  FunctionImplementation
> = {
  // Non-aggregate functions
  DATE: (arg) => {
    throw new Error('not implemented')
  },
  JSON_EXTRACT: (arg) => {
    throw new Error('not implemented')
  },
  UPPER: (arg) => {
    throw new Error('not implemented')
  },
  LOWER: (arg) => {
    throw new Error('not implemented')
  },
  COALESCE: (arg) => {
    throw new Error('not implemented')
  },
  CONCAT: (arg) => {
    throw new Error('not implemented')
  },
  LENGTH: (arg) => {
    throw new Error('not implemented')
  },
}

/**
 * Evaluates a function call with the given name and arguments
 * @param functionName The name of the function to evaluate
 * @param arg The arguments to pass to the function
 * @returns The result of the function call
 */
export function evaluateFunction(
  functionName: AllowedFunctionName,
  arg: unknown,
): unknown {
  const implementation = functionImplementations[functionName]
  if (!implementation) {
    throw new Error(`Unknown function: ${functionName}`)
  }
  return implementation(arg)
}

/**
 * Determines if an object is a function call
 * @param obj The object to check
 * @returns True if the object is a function call, false otherwise
 */
export function isFunctionCall(obj: unknown): boolean {
  if (!obj || typeof obj !== 'object') {
    return false
  }

  const keys = Object.keys(obj)
  if (keys.length !== 1) {
    return false
  }

  const functionName = keys[0] as string

  // Check if the key is one of the allowed function names
  return Object.keys(functionImplementations).includes(functionName)
}

/**
 * Extracts the function name and argument from a function call object.
 */
export function extractFunctionCall(obj: Record<string, unknown>): {
  functionName: AllowedFunctionName
  argument: unknown
} {
  const keys = Object.keys(obj)
  if (keys.length !== 1) {
    throw new Error('Invalid function call: object must have exactly one key')
  }

  const functionName = keys[0] as AllowedFunctionName
  if (!Object.keys(functionImplementations).includes(functionName)) {
    throw new Error(`Invalid function name: ${functionName}`)
  }

  return {
    functionName,
    argument: obj[functionName],
  }
}
