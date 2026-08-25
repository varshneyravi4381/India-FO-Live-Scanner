# India F&O Scanner v27 Cloud Edition

This version keeps the v26 scanner behavior and adds optional cloud-safe history backup.

## Why remote backup exists
Free web hosts may restart or discard their local filesystem. v27 can mirror the complete `data/` directory (SQLite + JSON history) to a private Supabase Storage bucket and restore it automatically after a restart.

## Required environment variables on the cloud host
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `SUPABASE_BUCKET=scanner-backups`
- `SUPABASE_OBJECT=v27/data-backup.zip`

If these are absent, the scanner behaves like v26 and uses local `data/` only.

## Supabase setup
1. Create a Supabase project.
2. Storage -> create a PRIVATE bucket named `scanner-backups`.
3. Project Settings/API: copy Project URL and the server-side service-role key.
4. Put those values in the hosting provider's environment-variable settings. Never place the service-role key in browser JavaScript or share it publicly.

## Health check
Open `/api/cloud/health`. `remote_history_configured` should be `true` after environment variables are set.

## Important scheduling note for free hosts
A free host that sleeps cannot guarantee an in-process Python scheduler fires exactly at 09:22/EOD. Use an external scheduler to call the existing capture endpoints at the required IST times, and optionally call `/api/cloud/backup-now` immediately afterward. This also wakes the service if the provider has put it to sleep.
