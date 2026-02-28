# Extract from CEA Website

This module extracts Singapore real estate agent data from the Council for Estate Agencies (CEA) website.

## Usage

```bash
# Install dependencies
uv sync

# Run the scraper
uv run python main.py directory  # Scrape agent directory
uv run python main.py details    # Scrape agent details
uv run python main.py directory details  # Both
```

## Commands

- `directory` - Scrapes the agent directory using vowel-based filtering
- `details` - Scrapes detailed agent information from registration numbers
  - `-rn` / `--start_registration` - Start from a specific registration number (e.g., R012345A)
