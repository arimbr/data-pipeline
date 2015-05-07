#!/bin/sh
cd /home/ubuntu/data-pipeline

# Export id, title, tags from jobs collection sorted by date
mongoexport -d stackoverflow -c jobs -f id,title,tags -q '{$query: {date: {$exists: 1}}, $orderby: {date: -1}}' > items_dump.jl

# Save last 5000 jobs 
head -n 5000 items_dump.jl > items_sample.jl

# Clean jobs and tags, and convert from .jl to .json
python clean.py items_sample.jl items_clean.json

# Sync data with S3
s3cmd sync --acl-public items_clean.json s3://arimbr
