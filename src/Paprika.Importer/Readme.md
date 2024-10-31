## Paprika Importer

Use the following steps to import from Nethermind’s DB

-   Switch to the `importer` branch
	- `git checkout remotes/origin/importer`
-   Fetch the submodules
    -   `git submodule update --init --recursive`
-   Build Paprika.Importer
    -   `dotnet build .\\src\\Paprika.Importer\\Paprika.Importer.sln`
-   Run
    -   `cd .\\src\\Paprika.Importer`
    -   `dotnet run -- path_to_nethermind_db_mainnet`