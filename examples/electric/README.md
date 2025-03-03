# ElectricSQL Example with D2TS

This example demonstrates how to use [D2TS](https://github.com/electric-sql/d2ts) with [ElectricSQL](https://electric-sql.com) to build a real-time data pipeline that processes changes incrementally. The example implements an issue tracking system with users, issues, and comments.

## Overview

This example showcases:

1. Setting up ElectricSQL with PostgreSQL
2. Building an incremental data pipeline using D2TS
3. Performing joins, aggregations, and transformations on real-time data
4. Processing data changes efficiently with differential dataflow

## Architecture

The example consists of:

- A PostgreSQL database paired with ElectricSQL for syncing out changes in real-time
- Schema for users, issues, and comments
- A D2TS pipeline that:
  - Joins issues with their creators (users)
  - Counts comments for each issue
  - Consolidates the data into a unified view

## Data Model

The example implements a simple issue tracking system with:

- **Users**: People who create issues and comments
- **Issues**: Tasks or bugs with properties like priority and status
- **Comments**: Text comments on issues

## Prerequisites

- Node.js and pnpm
- Docker and Docker Compose (for running PostgreSQL and ElectricSQL)

## Setup and Running

1. **Start the backend services**

   ```bash
   pnpm backend:up
   ```

   This starts PostgreSQL and ElectricSQL services using Docker Compose.

2. **Set up the database**

   ```bash
   pnpm db:migrate
   ```

   This applies the database migrations to create the necessary tables.

3. **Load sample data**

   ```bash
   pnpm db:load-data
   ```

   This loads sample users, issues, and comments data into the database.

4. **Run the example**

   ```bash
   pnpm start
   ```

   This starts the D2TS pipeline that consumes data from ElectricSQL and processes it incrementally. The pipeline will output the processed data to the console.

5. **Reset everything (optional)**

   ```bash
   pnpm reset
   ```

   This command tears down the services, recreates them, applies migrations, and loads fresh data.

## How It Works

The pipeline in `src/index.ts` demonstrates:

1. **Creating a D2TS graph** - The foundation for the data processing pipeline
2. **Setting up inputs** - Connecting ElectricSQL shape streams to D2TS inputs
3. **Building transformations**:
   - Calculating comment counts per issue
   - Joining issues with their creators
   - Transforming and consolidating the data
4. **Consuming the results** - Outputting processed data as it changes

The ElectricSQL integration uses `MultiShapeStream` to consume multiple shapes (one per table) from the same Electric instance and the `electricStreamToD2Input` helper to connect Electric streams to D2TS inputs.

## Key Concepts

- **Differential Dataflow**: D2TS enables incremental computations, only reprocessing what's changed
- **ElectricSQL ShapeStreams**: Real-time data streams that emit changes to the database
- **LSN-based Processing**: Changes are processed based on PostgreSQL Log Sequence Numbers (LSNs) for consistency - these are used at the "version" of the data passed to D2TS

## Resources

- [D2TS Documentation](https://github.com/electric-sql/d2ts)
- [ElectricSQL Documentation](https://electric-sql.com/docs)
- [Differential Dataflow Paper](https://github.com/frankmcsherry/blog/blob/master/posts/2015-09-29.md)

