#!/usr/bin/env bun
/**
 * CLI tool for building the nationwide address database.
 *
 * Commands: urls - Generate list of TIGER ADDRFEAT URLs download - Download
 * ADDRFEAT files from URL list build - Build SQLite database from downloaded
 * files gazetteer - Download ZCTA gazetteer for ZIP centroids
 */

import { Database } from "bun:sqlite"
import fs from "node:fs/promises"
import path from "node:path"
import { Glob } from "bun"
import { program } from "commander"
import { DBFFile } from "dbffile"
import { doubleMetaphone } from "double-metaphone"
import pLimit from "p-limit"
import { bufferCount, from, lastValueFrom, mergeMap, tap } from "rxjs"
import * as yauzl from "yauzl-promise"
import packageJson from "../package.json"

const TIGER_ADDRFEAT_URL =
  "https://www2.census.gov/geo/tiger/TIGER2024/ADDRFEAT"
const TIGER_FEATNAMES_URL =
  "https://www2.census.gov/geo/tiger/TIGER2024/FEATNAMES"
const GAZETTEER_URL =
  "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip"

// ─────────────────────────────────────────────────────────────────────────────
// URL List Command
// ─────────────────────────────────────────────────────────────────────────────

async function generateUrls(output: string) {
  console.log("Fetching file lists from Census Bureau...")

  // Fetch ADDRFEAT URLs
  const addrfeatResponse = await fetch(TIGER_ADDRFEAT_URL)
  const addrfeatHtml = await addrfeatResponse.text()
  const addrfeatMatches = addrfeatHtml.match(/tl_2024_\d+_addrfeat\.zip/g) ?? []
  const addrfeatUnique = [...new Set(addrfeatMatches)]
  const addrfeatUrls = addrfeatUnique.map((f) => `${TIGER_ADDRFEAT_URL}/${f}`)

  // Fetch FEATNAMES URLs
  const featnamesResponse = await fetch(TIGER_FEATNAMES_URL)
  const featnamesHtml = await featnamesResponse.text()
  const featnamesMatches =
    featnamesHtml.match(/tl_2024_\d+_featnames\.zip/g) ?? []
  const featnamesUnique = [...new Set(featnamesMatches)]
  const featnamesUrls = featnamesUnique.map(
    (f) => `${TIGER_FEATNAMES_URL}/${f}`,
  )

  const allUrls = [...addrfeatUrls, ...featnamesUrls]

  console.log(
    `Found ${addrfeatUrls.length} ADDRFEAT + ${featnamesUrls.length} FEATNAMES files`,
  )

  if (output === "-") {
    for (const url of allUrls) console.log(url)
  } else {
    await Bun.write(output, allUrls.join("\n") + "\n")
    console.log(`Written to ${output}`)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Download Command
// ─────────────────────────────────────────────────────────────────────────────

async function checkUnzipInstalled() {
  const result = await Bun.$`which unzip`.quiet().nothrow()
  if (result.exitCode !== 0) {
    throw new Error("unzip is required but not installed. Please install it.")
  }
}

async function downloadFiles(
  urlFile: string,
  outputDir: string,
  concurrency: number,
) {
  await checkUnzipInstalled()

  const content = await Bun.file(urlFile).text()
  const allUrls = content.trim().split("\n").filter(Boolean)

  // Separate ADDRFEAT and FEATNAMES URLs
  const addrfeatUrls = allUrls.filter((u) => u.includes("addrfeat"))
  const featnamesUrls = allUrls.filter((u) => u.includes("featnames"))

  console.log(
    `Downloading ${addrfeatUrls.length} ADDRFEAT + ${featnamesUrls.length} FEATNAMES files...`,
  )
  await fs.mkdir(outputDir, { recursive: true })

  const limit = pLimit(concurrency)
  let completed = 0
  let errors = 0
  const total = allUrls.length

  const tasks = allUrls.map((url) =>
    limit(async () => {
      const filename = path.basename(url)
      const outputPath = path.join(outputDir, filename)

      // Skip if already exists
      try {
        await fs.access(outputPath)
        completed++
        return
      } catch {
        // File doesn't exist, download it
      }

      try {
        const response = await fetch(url)
        if (!response.ok) throw new Error(`HTTP ${response.status}`)
        const buffer = await response.arrayBuffer()
        await Bun.write(outputPath, buffer)
        completed++
        const pct = ((completed / total) * 100).toFixed(1)
        process.stdout.write(`\r[${pct}%] Downloaded ${filename}`.padEnd(60))
      } catch (err) {
        errors++
        console.error(`\nFailed: ${filename} - ${String(err)}`)
      }
    }),
  )

  await Promise.all(tasks)
  console.log(`\nComplete. Downloaded: ${completed}, Errors: ${errors}`)

  // Extract DBF files to separate directories
  console.log("\nExtracting DBF files...")

  const addrfeatDir = path.join(outputDir, "addrfeat")
  const featnamesDir = path.join(outputDir, "featnames")
  await fs.mkdir(addrfeatDir, { recursive: true })
  await fs.mkdir(featnamesDir, { recursive: true })

  // Extract ADDRFEAT DBFs
  await Bun.$`cd ${outputDir} && ls *_addrfeat.zip 2>/dev/null | xargs -P ${concurrency} -I {} unzip -jo {} "*.dbf" -d addrfeat/`
    .quiet()
    .nothrow()

  // Extract FEATNAMES DBFs
  await Bun.$`cd ${outputDir} && ls *_featnames.zip 2>/dev/null | xargs -P ${concurrency} -I {} unzip -jo {} "*.dbf" -d featnames/`
    .quiet()
    .nothrow()

  const addrfeatCount = await Bun.$`ls ${addrfeatDir}/*.dbf 2>/dev/null | wc -l`
    .quiet()
    .text()
  const featnamesCount =
    await Bun.$`ls ${featnamesDir}/*.dbf 2>/dev/null | wc -l`.quiet().text()

  console.log(
    `Extracted ${addrfeatCount.trim()} ADDRFEAT DBFs to ${addrfeatDir}`,
  )
  console.log(
    `Extracted ${featnamesCount.trim()} FEATNAMES DBFs to ${featnamesDir}`,
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Gazetteer Command
// ─────────────────────────────────────────────────────────────────────────────

async function downloadGazetteer(outputPath: string) {
  console.log("Downloading ZCTA gazetteer...")

  const response = await fetch(GAZETTEER_URL)
  if (!response.ok) throw new Error(`HTTP ${response.status}`)

  const zipBuffer = Buffer.from(await response.arrayBuffer())
  const zipfile = await yauzl.fromBuffer(zipBuffer)

  try {
    for await (const entry of zipfile) {
      if (entry.filename.endsWith(".txt")) {
        const readStream = await zipfile.openReadStream(entry)
        const chunks: Buffer[] = []
        for await (const chunk of readStream) chunks.push(chunk)

        await fs.mkdir(path.dirname(outputPath), { recursive: true })
        await Bun.write(outputPath, Buffer.concat(chunks))
        console.log(`Extracted to ${outputPath}`)
        return
      }
    }
  } finally {
    await zipfile.close()
  }

  throw new Error("No .txt file found in gazetteer zip")
}

// ─────────────────────────────────────────────────────────────────────────────
// Build Command
// ─────────────────────────────────────────────────────────────────────────────

interface ZipCentroid {
  zip: string
  lat: number
  lon: number
}

/** Structured street name from FEATNAMES. */
interface StreetName {
  prefix: string | null // PREDIRABRV - direction prefix (N, S, E, W)
  name: string // NAME - the street name
  type: string | null // SUFTYPABRV - street type (St, Ave, Rd)
  suffix: string | null // SUFDIRABRV - direction suffix
}

/** Processed street ready for database insertion. */
interface ProcessedStreet {
  stateFips: string
  countyFips: string
  prefix: string | null
  name: string
  type: string | null
  suffix: string | null
  metaphonePrimary: string
  metaphoneSecondary: string | null
  zips: string[]
}

/** Creates a unique key for a street name variant. */
function streetNameKey(s: StreetName): string {
  return `${s.prefix ?? ""}|${s.name}|${s.type ?? ""}|${s.suffix ?? ""}`
}

/**
 * Loads all FEATNAMES DBFs into a Map keyed by LINEARID, collecting all unique
 * name variants per LINEARID.
 */
async function loadFeatnamesLookup(
  featnamesDir: string,
  concurrency: number,
): Promise<Map<string, Set<string>>> {
  const glob = new Glob("tl_2024_*_featnames.dbf")
  const files: string[] = []
  for await (const file of glob.scan(featnamesDir)) {
    files.push(path.join(featnamesDir, file))
  }

  console.log(`Loading ${files.length} FEATNAMES files...`)

  // Map: LINEARID -> Set of streetNameKey strings
  const lookup = new Map<string, Set<string>>()
  const limit = pLimit(concurrency)
  let filesProcessed = 0

  const tasks = files.map((file) =>
    limit(async () => {
      try {
        const dbf = await DBFFile.open(file)
        let batch: Array<Record<string, unknown>>

        while ((batch = await dbf.readRecords(1000)).length > 0) {
          for (const record of batch) {
            const linearId = (record.LINEARID as string | undefined)?.trim()
            const name = (record.NAME as string | undefined)?.trim()
            if (!linearId || !name) continue

            const streetName: StreetName = {
              prefix: (record.PREDIRABRV as string | undefined)?.trim() || null,
              name,
              type: (record.SUFTYPABRV as string | undefined)?.trim() || null,
              suffix: (record.SUFDIRABRV as string | undefined)?.trim() || null,
            }

            const key = streetNameKey(streetName)
            const existing = lookup.get(linearId)
            if (existing) {
              existing.add(key)
            } else {
              lookup.set(linearId, new Set([key]))
            }
          }
        }
      } catch {
        // Skip files that fail to open
      }

      filesProcessed++
      const pct = ((filesProcessed / files.length) * 100).toFixed(1)
      process.stdout.write(
        `\r[${pct}%] Loaded ${filesProcessed}/${files.length} FEATNAMES files`.padEnd(
          70,
        ),
      )
    }),
  )

  await Promise.all(tasks)
  console.log(`\nLoaded ${lookup.size} unique LINEARIDs`)

  return lookup
}

/** Parses a streetNameKey back into a StreetName object. */
function parseStreetNameKey(key: string): StreetName {
  const [prefix, name, type, suffix] = key.split("|")
  return {
    prefix: prefix || null,
    name: name ?? "",
    type: type || null,
    suffix: suffix || null,
  }
}

/**
 * Processes a single ADDRFEAT DBF file, joining with FEATNAMES lookup to get
 * structured name fields.
 */
async function processAddrfeatFile(
  dbfPath: string,
  featnamesLookup: Map<string, Set<string>>,
): Promise<ProcessedStreet[]> {
  const filename = path.basename(dbfPath)

  const fipsMatch = filename.match(/tl_2024_(\d{2})(\d{3})_addrfeat\.dbf/)
  if (!fipsMatch?.[1] || !fipsMatch[2]) return []

  const stateFips = fipsMatch[1]
  const countyFips = fipsMatch[2]

  // Map: streetNameKey -> { zips }
  const streetMap = new Map<string, Set<string>>()

  try {
    const dbf = await DBFFile.open(dbfPath)
    let batch: Array<Record<string, unknown>>

    while ((batch = await dbf.readRecords(1000)).length > 0) {
      for (const record of batch) {
        const linearId = (record.LINEARID as string | undefined)?.trim()
        if (!linearId) continue

        const zipL = (record.ZIPL as string | undefined)?.trim() || null
        const zipR = (record.ZIPR as string | undefined)?.trim() || null
        if (!zipL && !zipR) continue

        // Look up structured names from FEATNAMES
        const nameKeys = featnamesLookup.get(linearId)
        if (!nameKeys || nameKeys.size === 0) continue

        // Add ZIPs for each name variant
        for (const nameKey of nameKeys) {
          const fullKey = `${stateFips}|${countyFips}|${nameKey}`
          const existing = streetMap.get(fullKey)
          if (existing) {
            if (zipL) existing.add(zipL)
            if (zipR) existing.add(zipR)
          } else {
            const zips = new Set<string>()
            if (zipL) zips.add(zipL)
            if (zipR) zips.add(zipR)
            streetMap.set(fullKey, zips)
          }
        }
      }
    }
  } catch {
    return []
  }

  // Convert to processed streets with metaphone on NAME only
  const results: ProcessedStreet[] = []
  for (const [fullKey, zips] of streetMap) {
    // Parse out the streetNameKey part (skip stateFips|countyFips|)
    const nameKeyStart = fullKey.indexOf("|", fullKey.indexOf("|") + 1) + 1
    const nameKey = fullKey.slice(nameKeyStart)
    const streetName = parseStreetNameKey(nameKey)

    // Compute metaphone on just the name
    const [primary, secondary] = doubleMetaphone(streetName.name)

    results.push({
      stateFips,
      countyFips,
      prefix: streetName.prefix,
      name: streetName.name,
      type: streetName.type,
      suffix: streetName.suffix,
      metaphonePrimary: primary,
      metaphoneSecondary: secondary ?? null,
      zips: [...zips],
    })
  }

  return results
}

function parseGazetteer(content: string): ZipCentroid[] {
  const centroids: ZipCentroid[] = []
  const lines = content.split("\n")

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]?.trim()
    if (!line) continue

    const parts = line.split("\t")
    const zip = parts[0]?.trim()
    const lat = parseFloat(parts[5] ?? "")
    const lon = parseFloat(parts[6] ?? "")

    if (zip && !isNaN(lat) && !isNaN(lon)) {
      centroids.push({ zip, lat, lon })
    }
  }

  return centroids
}

async function buildDatabase(
  inputDir: string,
  gazetteerPath: string,
  outputPath: string,
  readConcurrency: number,
  writeBatchSize: number,
) {
  const startTime = Date.now()

  // Load FEATNAMES lookup first
  const featnamesDir = path.join(inputDir, "featnames")
  const featnamesLookup = await loadFeatnamesLookup(
    featnamesDir,
    readConcurrency,
  )

  // Find all extracted ADDRFEAT DBF files
  const addrfeatDir = path.join(inputDir, "addrfeat")
  const glob = new Glob("tl_2024_*_addrfeat.dbf")
  const files: string[] = []
  for await (const file of glob.scan(addrfeatDir)) {
    files.push(path.join(addrfeatDir, file))
  }
  console.log(`Found ${files.length} ADDRFEAT DBF files`)

  // Create database
  console.log(`Creating database at ${outputPath}...`)
  await fs.mkdir(path.dirname(outputPath), { recursive: true })
  for (const suffix of ["", "-wal", "-shm"]) {
    await fs.unlink(outputPath + suffix).catch(() => {})
  }

  const db = new Database(outputPath)
  db.run("PRAGMA journal_mode = WAL")
  db.run("PRAGMA synchronous = NORMAL")

  db.run(`
    CREATE TABLE IF NOT EXISTS streets (
      id INTEGER PRIMARY KEY,
      state_fips TEXT NOT NULL,
      county_fips TEXT NOT NULL,
      prefix TEXT,
      name TEXT NOT NULL,
      type TEXT,
      suffix TEXT,
      metaphone_primary TEXT NOT NULL,
      metaphone_secondary TEXT
    )
  `)
  db.run(`
    CREATE TABLE IF NOT EXISTS street_zips (
      street_id INTEGER NOT NULL,
      zip TEXT NOT NULL,
      FOREIGN KEY (street_id) REFERENCES streets(id)
    )
  `)
  db.run(`
    CREATE TABLE IF NOT EXISTS zip_centroids (
      zip TEXT PRIMARY KEY,
      lat REAL NOT NULL,
      lon REAL NOT NULL
    )
  `)

  // Load gazetteer
  console.log("Loading ZIP centroids...")
  const gazContent = await Bun.file(gazetteerPath).text()
  const centroids = parseGazetteer(gazContent)

  const insertCentroid = db.prepare(
    "INSERT OR REPLACE INTO zip_centroids (zip, lat, lon) VALUES (?, ?, ?)",
  )
  const centroidTx = db.transaction(() => {
    for (const { zip, lat, lon } of centroids) {
      insertCentroid.run(zip, lat, lon)
    }
  })
  centroidTx()
  console.log(`Inserted ${centroids.length} ZIP centroids`)

  // Prepared statements for batch writes
  const insertStreet = db.prepare(`
    INSERT INTO streets (state_fips, county_fips, prefix, name, type, suffix, metaphone_primary, metaphone_secondary)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `)
  const insertZip = db.prepare(
    "INSERT INTO street_zips (street_id, zip) VALUES (?, ?)",
  )

  let filesProcessed = 0
  let totalStreets = 0
  let totalZips = 0

  /** Write a batch of streets to the database in a single transaction. */
  const writeBatch = (batch: ProcessedStreet[]) => {
    const tx = db.transaction(() => {
      for (const street of batch) {
        insertStreet.run(
          street.stateFips,
          street.countyFips,
          street.prefix,
          street.name,
          street.type,
          street.suffix,
          street.metaphonePrimary,
          street.metaphoneSecondary,
        )
        const streetId = db.query("SELECT last_insert_rowid() as id").get() as {
          id: number
        }
        for (const zip of street.zips) {
          insertZip.run(streetId.id, zip)
          totalZips++
        }
        totalStreets++
      }
    })
    tx()
  }

  console.log(
    `Processing ADDRFEAT files (read concurrency: ${readConcurrency}, write batch: ${writeBatchSize})...`,
  )

  // RxJS pipeline: read ADDRFEAT files, join with FEATNAMES, batch writes
  await lastValueFrom(
    from(files).pipe(
      // Read and process ADDRFEAT files with concurrency, joining with FEATNAMES
      mergeMap(async (file) => {
        const streets = await processAddrfeatFile(file, featnamesLookup)
        filesProcessed++
        const pct = ((filesProcessed / files.length) * 100).toFixed(1)
        process.stdout.write(
          `\r[${pct}%] Read ${filesProcessed}/${files.length} files, ${totalStreets} streets`.padEnd(
            70,
          ),
        )
        return streets
      }, readConcurrency),
      // Flatten arrays of streets into individual streets
      mergeMap((streets) => from(streets)),
      // Buffer into batches for efficient DB writes
      bufferCount(writeBatchSize),
      // Write each batch
      tap((batch) => writeBatch(batch)),
    ),
    { defaultValue: undefined },
  )

  console.log("\n")

  // Create indexes
  console.log("Creating indexes...")
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_streets_metaphone_primary ON streets(metaphone_primary)",
  )
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_streets_metaphone_secondary ON streets(metaphone_secondary)",
  )
  db.run("CREATE INDEX IF NOT EXISTS idx_street_zips_zip ON street_zips(zip)")
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_street_zips_street_id ON street_zips(street_id)",
  )

  // Optimize
  console.log("Optimizing...")
  db.run("PRAGMA optimize")
  db.run("VACUUM")

  db.close()

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1)
  console.log(`\nBuild complete in ${elapsed} minutes`)
  console.log(`  Streets: ${totalStreets}`)
  console.log(`  Street-ZIP links: ${totalZips}`)
  console.log(`  ZIP centroids: ${centroids.length}`)
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI Setup
// ─────────────────────────────────────────────────────────────────────────────

program
  .name("phonetic-tiger")
  .description("CLI tool for building the nationwide address database")
  .version(packageJson.version)

program
  .command("urls")
  .description("Generate list of TIGER ADDRFEAT URLs")
  .option("-o, --output <file>", "Output file (use - for stdout)", "urls.txt")
  .action(async (opts) => {
    await generateUrls(opts.output)
  })

program
  .command("download")
  .description("Download ADDRFEAT files from URL list")
  .option("-i, --input <file>", "URL list file", "./urls.txt")
  .option("-o, --output <dir>", "Output directory", "./addrfeat")
  .option("-c, --concurrency <n>", "Download concurrency", "10")
  .action(async (opts) => {
    await downloadFiles(opts.input, opts.output, parseInt(opts.concurrency))
  })

program
  .command("gazetteer")
  .description("Download ZCTA gazetteer for ZIP centroids")
  .option("-o, --output <file>", "Output file", "./gazetteer.txt")
  .action(async (opts) => {
    await downloadGazetteer(opts.output)
  })

program
  .command("build")
  .description("Build SQLite database from downloaded files")
  .option(
    "-i, --input <dir>",
    "Directory containing ADDRFEAT zips",
    "./addrfeat",
  )
  .option("-g, --gazetteer <file>", "Gazetteer file path", "./gazetteer.txt")
  .option("-o, --output <file>", "Output database path", "./addresses.db")
  .option("-c, --concurrency <n>", "DBF read concurrency", "10")
  .option("-b, --batch-size <n>", "DB write batch size", "5000")
  .action(async (opts) => {
    await buildDatabase(
      opts.input,
      opts.gazetteer,
      opts.output,
      parseInt(opts.concurrency),
      parseInt(opts.batchSize),
    )
  })

program.parse()
