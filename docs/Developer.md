# Deveoeper Guide

## Mongo

### Create a Database User in Atlas

   1. Log in to MongoDB Atlas.
   2. In the left-hand sidebar, under Security, click Database Access.
   3. Click the + Add New Database User button.
   4. Authentication Method: Keep it as "Password".
   5. Password Management: Enter a username (e.g., backup-user) and a secure password. 
   6. Tip: Use the "Autogenerate Secure Password" button and copy it immediately. 
   7. Database User Privileges -> Built-in Role: Ensure "Read and write to any database" (or specific permissions) is selected. 
   8. Click Add User.

### Dump from server

```
mongosh "mongodb+srv://cluster0.dearuu.mongodb.net/" --apiVersion 1 --username <db_username>
mongodump --uri="mongodb+srv://<user>:<password>@cluster0.dearuu.mongodb.net/leases" --out="./30012026"
```

### Load dump to local Mongo

```
mongorestore --uri="mongodb://localhost:27017" ./30012026/
```

### Local mongo Start

```
brew services start mongodb/brew/mongodb-community
```

### Export locally:

1. In your local Compass, select the collection you changed.
2. Click the Export Data tab.
3. Select JSON (ensure "Export full collection" is checked) and save the file to your desktop.

or For big size collections, use the command line:

```
mongodump \
  --uri="mongodb://localhost:27017" \
  --db=leases \
  --collection=leases \
  --out=./local_dump_24022026
```

### Load dump to Atlas

Warning: This will drop the existing collection in Atlas before importing the new data. Ensure you have a backup if needed.

Warning: Check the indexes in Atlas before importing, as they may need to be recreated after the import.

leases:
_id -> asc (1), Unique
uid -> asc (1)
pc -> asc (1)

leasesext:
lid -> asc (1)

In terminal:
```bash
mongoimport --uri="mongodb+srv://<user>:<password>@{SERVER}/leases" \
            --collection=leases \
            --file="path/to/your_exported_file.json" \
            --jsonArray \
            --drop
```

or  if bson export (mongodump) is used (folder structure should be like: `./local_dump_24022026/leases/leases.bson`):

```
mongorestore --uri="mongodb+srv://<user>:<password>@{SERVER}" \
            --nsInclude="leases.leases" \
            --drop \
            ./local_dump_24022026
```

### Set Up Search Indexes in MongoDB Atlas

The application requires three search indexes for optimal performance:
- `default`
- `addr_autocomplete`
- `postcode_autocomplete`

These must be set up manually in **MongoDB Atlas**:

1. Log in to your MongoDB Atlas account and navigate to your cluster.
2. Go to the **"Search"** tab for your database.
3. Click **"Create Search Index"**.
4. For each index:
   - Select the `leases` collection.
   - Choose **"JSON Editor"** mode.
   - Copy the JSON definition from the corresponding file in `data/atlas-search-indexes/` (`default.json`, `addr_autocomplete.json`, or `postcode_autocomplete.json`).
   - Paste it into the editor and create the index.

Repeat for all three indexes: `default`, `addr_autocomplete`, and `postcode_autocomplete`.

### Create Field Indexes in MongoDB Atlas

The following field indexes should be created in MongoDB Atlas for the `leases` collection to optimize query performance:

- leasetermcaches
    - Index: term
- leaseviewstats
    - Index: uniqueId
- searchanalytics
    - Index: type
- userloginstats
    - Index: period
- users
    - Index: guid, lastLogin, verificationCode.expiresAt, searchHistory.timestamp
