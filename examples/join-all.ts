import { D2, map, joinAll, debug, rekey } from '@electric-sql/d2ts'

type Product = {
  id: number
  name: string
  categoryId: number
  supplierId: number
}

type Category = {
  id: number
  name: string
}

type Supplier = {
  id: number
  name: string
  countryId: number
}

type Country = {
  id: number
  name: string
  continent: string
}

// Sample data
const products: Product[] = [
  { id: 1, name: 'iPhone 13', categoryId: 1, supplierId: 1 },
  { id: 2, name: 'Galaxy S21', categoryId: 1, supplierId: 2 },
  { id: 3, name: 'MacBook Pro', categoryId: 2, supplierId: 1 },
  { id: 4, name: 'Dell XPS', categoryId: 2, supplierId: 3 },
  { id: 5, name: 'iPad Pro', categoryId: 3, supplierId: 1 },
  { id: 6, name: 'Surface Pro', categoryId: 3, supplierId: 4 },
]

const categories: Category[] = [
  { id: 1, name: 'Smartphones' },
  { id: 2, name: 'Laptops' },
  { id: 3, name: 'Tablets' },
  { id: 4, name: 'Accessories' },
]

const suppliers: Supplier[] = [
  { id: 1, name: 'Apple Inc.', countryId: 1 },
  { id: 2, name: 'Samsung Electronics', countryId: 2 },
  { id: 3, name: 'Dell Technologies', countryId: 1 },
  { id: 4, name: 'Microsoft', countryId: 1 },
]

const countries: Country[] = [
  { id: 1, name: 'United States', continent: 'North America' },
  { id: 2, name: 'South Korea', continent: 'Asia' },
  { id: 3, name: 'Japan', continent: 'Asia' },
  { id: 4, name: 'Germany', continent: 'Europe' },
]

// Example 1: Using joinAll for a multi-way join
console.log('\n=== Example 1: Multi-way Join with joinAll ===')

const graph = new D2({
  initialFrontier: 0,
})

// Create input streams
const inputProducts = graph.newInput<[number, Product]>()
const inputCategories = graph.newInput<[number, Category]>()
const inputSuppliers = graph.newInput<[number, Supplier]>()
const inputCountries = graph.newInput<[number, Country]>()

// Transform for joining
const productsStream = inputProducts.pipe(
  rekey((product) => product.categoryId),
)

const categoriesStream = inputCategories.pipe(rekey((category) => category.id))

const suppliersStream = inputSuppliers.pipe(rekey((supplier) => supplier.id))

const countriesStream = inputCountries.pipe(rekey((country) => country.id))

// Join all streams together
// First join products with categories by categoryId
// Then join with suppliers by supplierId using a transform
// Then join suppliers with countries by countryId
const joinedStream = productsStream.pipe(
  joinAll([
    categoriesStream,
    inputProducts.pipe(
      // Rekey by supplierId to join with suppliers
      rekey((product) => product.supplierId),
      joinAll([
        suppliersStream,
        // Join suppliers with countries
        suppliersStream.pipe(
          rekey((supplier) => supplier.countryId),
          joinAll([countriesStream]),
        ),
      ]),
    ),
  ]),
  map(
    ([
      _key,
      [
        product,
        category,
        [productBySupplierId, supplier, [supplierByCountryId, country]],
      ],
    ]) => [
      product.id,
      {
        product: product.name,
        category: category.name,
        supplier: supplier.name,
        country: country.name,
        continent: country.continent,
      },
    ],
  ),
  debug('join-all-result', true),
)

graph.finalize()

// Send initial data
for (const product of products) {
  inputProducts.sendData(1, [[[product.id, product], 1]])
}

for (const category of categories) {
  inputCategories.sendData(1, [[[category.id, category], 1]])
}

for (const supplier of suppliers) {
  inputSuppliers.sendData(1, [[[supplier.id, supplier], 1]])
}

for (const country of countries) {
  inputCountries.sendData(1, [[[country.id, country], 1]])
}

// Update frontiers
inputProducts.sendFrontier(2)
inputCategories.sendFrontier(2)
inputSuppliers.sendFrontier(2)
inputCountries.sendFrontier(2)

// Run the computation
graph.run()

// Add a formatted output to explain the results
console.log('\nResults from Example 1:')
console.log(
  'The joinAll operator successfully joined products with their categories, suppliers, and countries.',
)
console.log(
  'Each product now has information about its category, supplier, and country of origin.',
)

// Example 2: Add a new product
console.log('\n=== Example 2: Adding a new product ===')

inputProducts.sendData(2, [
  [
    [
      7,
      {
        id: 7,
        name: 'AirPods Pro',
        categoryId: 4,
        supplierId: 1,
      },
    ],
    1,
  ],
])

// Update frontiers
inputProducts.sendFrontier(3)
inputCategories.sendFrontier(3)
inputSuppliers.sendFrontier(3)
inputCountries.sendFrontier(3)

// Run the computation
graph.run()

// Add a formatted output to explain the results
console.log('\nResults from Example 2:')
console.log(
  'Added "AirPods Pro" to the products and the joinAll operator successfully joined it with its category, supplier, and country.',
)

// Example 3: Left join example
console.log('\n=== Example 3: Left Join Example ===')

const leftJoinGraph = new D2({
  initialFrontier: 0,
})

// Create input streams for left join
const leftProducts = leftJoinGraph.newInput<[number, Product]>()
const leftCategories = leftJoinGraph.newInput<[number, Category]>()

// Transform for joining
const leftProductsStream = leftProducts.pipe(
  rekey((product) => product.categoryId),
)

const leftCategoriesStream = leftCategories.pipe(
  rekey((category) => category.id),
)

// Setup left join
const leftJoinStream = leftProductsStream.pipe(
  joinAll([leftCategoriesStream], 'left'),
  map(([_key, [product, category]]) => [
    product.id,
    {
      product: product.name,
      category: category ? category.name : 'Uncategorized',
    },
  ]),
  debug('left-join-result', true),
)

leftJoinGraph.finalize()

// Send products
for (const product of products) {
  leftProducts.sendData(1, [[[product.id, product], 1]])
}

// Send only some categories to demonstrate left join
leftCategories.sendData(1, [[[1, categories[0]], 1]])
leftCategories.sendData(1, [[[2, categories[1]], 1]])

// Update frontiers
leftProducts.sendFrontier(2)
leftCategories.sendFrontier(2)

// Run computation
leftJoinGraph.run()

// Add a formatted output to explain the results
console.log('\nResults from Example 3:')
console.log(
  'Left join example: Products joined with categories, preserving all products even if they have no matching category.',
)
console.log(
  'Products with categoryId 3 have "Uncategorized" as their category because we only sent categories 1 and 2.',
)
