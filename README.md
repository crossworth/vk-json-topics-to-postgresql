##  VK JSON Topics to PostgreSQL

This is a companion tool for [VK Group Backup](https://github.com/crossworth/vk-group-backup).
It will read a folder with VK Topics in JSON format and save/update a PostgreSQL server.


### Usage

#### Migrate database
This tool can migrate the database, just add `--migrate=true` to the of the command when running for the first time.


This is a command line app.

**Windows**
```bash
json-to-pg-windows-amd64.exe -folder=postgresql -mongo=postgres://postgres:root@localhost/database?sslmode=disable
```

**Linux**
```shell script
json-to-pg-linux-amd64 -folder=backup -postgresql=postgres://postgres:root@localhost/database?sslmode=disable
```

#### Workers
This tool will try save/update **10** files at time, you can change this value passing the argument `--workers=1`.
