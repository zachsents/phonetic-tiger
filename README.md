# phonetic-tiger

Build a phonetic street index from US Census TIGER data for fuzzy address matching.

## Usage

```bash
# Generate URL list for all TIGER ADDRFEAT files
phonetic-tiger urls -o urls.txt

# Download files
phonetic-tiger download -i urls.txt -o ./addrfeat -c 10

# Download ZIP centroid data
phonetic-tiger gazetteer -o gazetteer.txt

# Build SQLite database
phonetic-tiger build -i ./addrfeat -g gazetteer.txt -o addresses.db
```

## Install

```bash
bun install
bun run start -- --help
```

## Build executable

```bash
bun run build          # macOS
bun run build:linux    # Linux
```
