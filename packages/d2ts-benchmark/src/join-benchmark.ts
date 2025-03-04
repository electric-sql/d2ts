import { Suite } from './base'
import {
  D2,
  MultiSet,
  map,
  join,
  joinAll,
  rekey,
  output,
} from '@electric-sql/d2ts'
import {
  join as joinSql,
  joinAll as joinAllSql,
} from '@electric-sql/d2ts/sqlite'
import { BetterSQLite3Wrapper } from '@electric-sql/d2ts/sqlite'
import Database from 'better-sqlite3'

// Define our data types
interface Product {
  id: number
  name: string
  categoryId: number
}

interface Category {
  id: number
  name: string
}

// Data generation functions
const generateData = (size: number) => {
  // Products data
  const products: Product[] = Array.from({ length: size }, (_, i) => ({
    id: i,
    name: `Product ${i}`,
    categoryId: i % (size / 10 || 1), // Distribute products across categories
  }))

  // Categories data
  const categories: Category[] = Array.from(
    { length: size / 10 || 1 },
    (_, i) => ({
      id: i,
      name: `Category ${i}`,
    }),
  )

  return { products, categories }
}

// Context type for benchmarks
interface BenchmarkContext {
  products: Product[]
  categories: Category[]
  initialProductsSet: MultiSet<any>
  initialCategoriesSet: MultiSet<any>
  results: any[]
  graph: D2 | null
  productStream: any
  categoryStream: any
  additionalProducts: Product[]
  db?: Database.Database
  sqliteWrapper?: BetterSQLite3Wrapper
}

// Main benchmark function
function runBenchmark({
  initialSize = 1000,
  incrementalSize = 100,
  incrementalRuns = 10,
}: {
  initialSize?: number
  incrementalSize?: number
  incrementalRuns?: number
} = {}) {
  console.log('========================================')
  console.log('Join Operators Benchmark')
  console.log(
    `Initial size: ${initialSize}, Incremental size: ${incrementalSize}, Runs: ${incrementalRuns}`,
  )
  console.log('========================================')

  const suite = new Suite({
    name: 'Join Operations',
    totalRuns: 3,
    incrementalRuns,
    warmupRuns: 1,
  })

  // In-Memory Join Benchmark
  suite.add({
    name: 'In-Memory Join',
    setup: () => {
      const { products, categories } = generateData(initialSize)

      // Prepare data structures
      const initialProductsSet = new MultiSet(
        products.map((product) => [[product.id, product], 1]),
      )

      const initialCategoriesSet = new MultiSet(
        categories.map((category) => [[category.id, category], 1]),
      )

      return {
        products,
        categories,
        initialProductsSet,
        initialCategoriesSet,
        results: [],
        graph: null,
        productStream: null,
        categoryStream: null,
        additionalProducts: generateData(incrementalSize * incrementalRuns)
          .products,
      } as BenchmarkContext
    },
    firstRun: (ctx: BenchmarkContext) => {
      // Create a new graph
      ctx.graph = new D2({ initialFrontier: 0 })

      // Create input streams
      ctx.productStream = ctx.graph.newInput<[number, Product]>()
      ctx.categoryStream = ctx.graph.newInput<[number, Category]>()

      // Set up the join
      ctx.productStream.pipe(
        rekey((product: Product) => product.categoryId),
        join(
          ctx.categoryStream.pipe(rekey((category: Category) => category.id)),
        ),
        map(([_key, [product, category]]: [number, [Product, Category]]) => ({
          id: product.id,
          name: product.name,
          category: category.name,
        })),
        output((data) => {
          ctx.results.push(data)
        }),
      )

      // Finalize graph
      ctx.graph.finalize()

      // Send initial data
      for (const product of ctx.products) {
        ctx.productStream.sendData(1, [[[product.id, product], 1]])
      }

      for (const category of ctx.categories) {
        ctx.categoryStream.sendData(1, [[[category.id, category], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(2)
      ctx.categoryStream.sendFrontier(2)

      // Run graph
      ctx.graph.run()
    },
    incrementalRun: (ctx: BenchmarkContext, i: number) => {
      if (!ctx.graph || !ctx.productStream || !ctx.categoryStream) return

      // Get a batch of additional products
      const startIdx = i * incrementalSize
      const endIdx = startIdx + incrementalSize
      const additionalProducts = ctx.additionalProducts.slice(startIdx, endIdx)

      // Send additional products
      for (const product of additionalProducts) {
        ctx.productStream.sendData(i + 2, [[[product.id, product], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(i + 3)
      ctx.categoryStream.sendFrontier(i + 3)

      // Run graph
      ctx.graph.run()
    },
    teardown: (ctx: BenchmarkContext) => {
      ctx.graph = null
      ctx.productStream = null
      ctx.categoryStream = null
      ctx.results = []
    },
  })

  // SQLite Join Benchmark
  suite.add({
    name: 'SQLite Join',
    setup: () => {
      const { products, categories } = generateData(initialSize)

      // Prepare data structures
      const initialProductsSet = new MultiSet(
        products.map((product) => [[product.id, product], 1]),
      )

      const initialCategoriesSet = new MultiSet(
        categories.map((category) => [[category.id, category], 1]),
      )

      // Create SQLite database
      const db = new Database(':memory:')
      const sqliteWrapper = new BetterSQLite3Wrapper(db)

      return {
        products,
        categories,
        initialProductsSet,
        initialCategoriesSet,
        results: [],
        graph: null,
        productStream: null,
        categoryStream: null,
        additionalProducts: generateData(incrementalSize * incrementalRuns)
          .products,
        db,
        sqliteWrapper,
      } as BenchmarkContext
    },
    firstRun: (ctx: BenchmarkContext) => {
      if (!ctx.sqliteWrapper) return

      ctx.graph = new D2({
        initialFrontier: 0,
        // @ts-ignore - D2 does support sqlite option but TypeScript doesn't know it
        sqlite: ctx.sqliteWrapper,
      })

      // Create input streams
      ctx.productStream = ctx.graph.newInput<[number, Product]>()
      ctx.categoryStream = ctx.graph.newInput<[number, Category]>()

      // Set up the join
      ctx.productStream.pipe(
        rekey((product: Product) => product.categoryId),
        // Pass only the SQLite wrapper to joinSql, remove the table names
        joinSql(
          ctx.categoryStream.pipe(rekey((category: Category) => category.id)),
          ctx.sqliteWrapper,
        ),
        map(([_key, [product, category]]: [number, [Product, Category]]) => ({
          id: product.id,
          name: product.name,
          category: category.name,
        })),
        output((data) => {
          ctx.results.push(data)
        }),
      )

      // Finalize graph
      ctx.graph.finalize()

      // Send initial data
      for (const product of ctx.products) {
        ctx.productStream.sendData(1, [[[product.id, product], 1]])
      }

      for (const category of ctx.categories) {
        ctx.categoryStream.sendData(1, [[[category.id, category], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(2)
      ctx.categoryStream.sendFrontier(2)

      // Run graph
      ctx.graph.run()
    },
    incrementalRun: (ctx: BenchmarkContext, i: number) => {
      if (!ctx.graph || !ctx.productStream || !ctx.categoryStream) return

      // Get a batch of additional products
      const startIdx = i * incrementalSize
      const endIdx = startIdx + incrementalSize
      const additionalProducts = ctx.additionalProducts.slice(startIdx, endIdx)

      // Send additional products
      for (const product of additionalProducts) {
        ctx.productStream.sendData(i + 2, [[[product.id, product], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(i + 3)
      ctx.categoryStream.sendFrontier(i + 3)

      // Run graph
      ctx.graph.run()
    },
    teardown: (ctx: BenchmarkContext) => {
      ctx.graph = null
      ctx.productStream = null
      ctx.categoryStream = null
      ctx.results = []
      ctx.db?.close()
    },
  })

  // In-Memory JoinAll Benchmark (just two streams to compare with regular join)
  suite.add({
    name: 'In-Memory JoinAll',
    setup: () => {
      const { products, categories } = generateData(initialSize)

      // Prepare data structures
      const initialProductsSet = new MultiSet(
        products.map((product) => [[product.id, product], 1]),
      )

      const initialCategoriesSet = new MultiSet(
        categories.map((category) => [[category.id, category], 1]),
      )

      return {
        products,
        categories,
        initialProductsSet,
        initialCategoriesSet,
        results: [],
        graph: null,
        productStream: null,
        categoryStream: null,
        additionalProducts: generateData(incrementalSize * incrementalRuns)
          .products,
      } as BenchmarkContext
    },
    firstRun: (ctx: BenchmarkContext) => {
      // Create a new graph
      ctx.graph = new D2({ initialFrontier: 0 })

      // Create input streams
      ctx.productStream = ctx.graph.newInput<[number, Product]>()
      ctx.categoryStream = ctx.graph.newInput<[number, Category]>()

      // Set up the join using joinAll with just two streams
      ctx.productStream.pipe(
        rekey((product: Product) => product.categoryId),
        joinAll([
          ctx.categoryStream.pipe(rekey((category: Category) => category.id)),
        ]),
        map(([_key, [product, category]]: [number, [Product, Category]]) => ({
          id: product.id,
          name: product.name,
          category: category.name,
        })),
        output((data) => {
          ctx.results.push(data)
        }),
      )

      // Finalize graph
      ctx.graph.finalize()

      // Send initial data
      for (const product of ctx.products) {
        ctx.productStream.sendData(1, [[[product.id, product], 1]])
      }

      for (const category of ctx.categories) {
        ctx.categoryStream.sendData(1, [[[category.id, category], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(2)
      ctx.categoryStream.sendFrontier(2)

      // Run graph
      ctx.graph.run()
    },
    incrementalRun: (ctx: BenchmarkContext, i: number) => {
      if (!ctx.graph || !ctx.productStream || !ctx.categoryStream) return

      // Get a batch of additional products
      const startIdx = i * incrementalSize
      const endIdx = startIdx + incrementalSize
      const additionalProducts = ctx.additionalProducts.slice(startIdx, endIdx)

      // Send additional products
      for (const product of additionalProducts) {
        ctx.productStream.sendData(i + 2, [[[product.id, product], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(i + 3)
      ctx.categoryStream.sendFrontier(i + 3)

      // Run graph
      ctx.graph.run()
    },
    teardown: (ctx: BenchmarkContext) => {
      ctx.graph = null
      ctx.productStream = null
      ctx.categoryStream = null
      ctx.results = []
    },
  })

  // SQLite JoinAll Benchmark (just two streams to compare with regular join)
  suite.add({
    name: 'SQLite JoinAll',
    setup: () => {
      const { products, categories } = generateData(initialSize)

      // Prepare data structures
      const initialProductsSet = new MultiSet(
        products.map((product) => [[product.id, product], 1]),
      )

      const initialCategoriesSet = new MultiSet(
        categories.map((category) => [[category.id, category], 1]),
      )

      // Create SQLite database
      const db = new Database(':memory:')
      const sqliteWrapper = new BetterSQLite3Wrapper(db)

      return {
        products,
        categories,
        initialProductsSet,
        initialCategoriesSet,
        results: [],
        graph: null,
        productStream: null,
        categoryStream: null,
        additionalProducts: generateData(incrementalSize * incrementalRuns)
          .products,
        db,
        sqliteWrapper,
      } as BenchmarkContext
    },
    firstRun: (ctx: BenchmarkContext) => {
      if (!ctx.sqliteWrapper) return

      // Create a new graph with SQLite support
      ctx.graph = new D2({
        initialFrontier: 0,
        // @ts-ignore - D2 does support sqlite option but TypeScript doesn't know it
        sqlite: ctx.sqliteWrapper,
      })

      // Create input streams
      ctx.productStream = ctx.graph.newInput<[number, Product]>()
      ctx.categoryStream = ctx.graph.newInput<[number, Category]>()

      // Set up the join using joinAll with just two streams
      ctx.productStream.pipe(
        rekey((product: Product) => product.categoryId),
        // Pass only the SQLite wrapper to joinAllSql, remove the table names
        joinAllSql(
          [ctx.categoryStream.pipe(rekey((category: Category) => category.id))],
          ctx.sqliteWrapper,
        ),
        map(([_key, [product, category]]: [number, [Product, Category]]) => ({
          id: product.id,
          name: product.name,
          category: category.name,
        })),
        output((data) => {
          ctx.results.push(data)
        }),
      )

      // Finalize graph
      ctx.graph.finalize()

      // Send initial data
      for (const product of ctx.products) {
        ctx.productStream.sendData(1, [[[product.id, product], 1]])
      }

      for (const category of ctx.categories) {
        ctx.categoryStream.sendData(1, [[[category.id, category], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(2)
      ctx.categoryStream.sendFrontier(2)

      // Run graph
      ctx.graph.run()
    },
    incrementalRun: (ctx: BenchmarkContext, i: number) => {
      if (!ctx.graph || !ctx.productStream || !ctx.categoryStream) return

      // Get a batch of additional products
      const startIdx = i * incrementalSize
      const endIdx = startIdx + incrementalSize
      const additionalProducts = ctx.additionalProducts.slice(startIdx, endIdx)

      // Send additional products
      for (const product of additionalProducts) {
        ctx.productStream.sendData(i + 2, [[[product.id, product], 1]])
      }

      // Update frontiers
      ctx.productStream.sendFrontier(i + 3)
      ctx.categoryStream.sendFrontier(i + 3)

      // Run graph
      ctx.graph.run()
    },
    teardown: (ctx: BenchmarkContext) => {
      ctx.graph = null
      ctx.productStream = null
      ctx.categoryStream = null
      ctx.results = []
      ctx.db?.close()
    },
  })

  // Run the benchmarks
  suite.run()
  suite.printResults()
}

// Run with default settings when executed directly
if (require.main === module) {
  runBenchmark()
}

export { runBenchmark }
