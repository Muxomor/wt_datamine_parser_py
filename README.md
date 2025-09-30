# War Thunder Tech Tree Parser

This project extracts and formats War Thunder vehicle data from datamined files into a structured format for database import. It sources its data from the [War Thunder Datamine](https://github.com/gszabi99/War-Thunder-Datamine) project.

The generated data is used by the [War Thunder Experience Calculator](https://github.com/Muxomor/WTExpCalc) and requires the database setup from the [WTExpCalc_db](https://github.com/Muxomor/WTExpCalc_db) repository.

-----

## Setup & Configuration

1.  **Prerequisites**: Ensure you have **Python 3.8+** installed.

2.  **Installation**: Clone the repository and install dependencies.

    ```bash
    git clone https://github.com/Muxomor/wt_datamine_parser_py
    cd wt_datamine_parser_py
    pip install requests pyjwt
    ```

3.  **Configuration**: Create a `config.txt` file in the root directory and add the necessary URLs and optional database credentials.

    ```ini
    shop_url=https://cdn.jsdelivr.net/gh/gszabi99/War-Thunder-Datamine@master/char.vromfs.bin_u/config/shop.blkx
    localization_url=https://cdn.jsdelivr.net/gh/gszabi99/War-Thunder-Datamine@master/lang.vromfs.bin_u/lang/units.csv
    wpcost_url=https://cdn.jsdelivr.net/gh/gszabi99/War-Thunder-Datamine@master/char.vromfs.bin_u/config/wpcost.blkx
    wpcost_fallback_urls=https://github.com/gszabi99/War-Thunder-Datamine/raw/master/char.vromfs.bin_u/config/wpcost.blkx
    rank_url=https://cdn.jsdelivr.net/gh/gszabi99/War-Thunder-Datamine@master/char.vromfs.bin_u/config/rank.blkx
    version_url=https://cdn.jsdelivr.net/gh/gszabi99/War-Thunder-Datamine@master/version

    base_url=http://localhost:3000
    parser_api_key=your_api_key
    jwt_secret=your_jwt_secret
    ```

-----

## Usage

The script is run via `main.py` with optional flags to control execution.

| Command | Description |
| :--- | :--- |
| `python main.py` | Runs the full parsing and merging pipeline. |
| `python main.py --shop-only` | Parses only the main tech tree structure. |
| `python main.py --localization-only` | Parses only localization data (requires `shop.csv`). |
| `python main.py --wpcost-only` | Parses only economic data (requires `shop.csv`). |
| `python main.py --misc-only` | Parses ranks, flags, images, and version. |
| `python main.py --merge-only` | Merges existing CSV files into final format. |
| `python main.py --db-upload` | Uploads generated data to the configured database. |
| `python main.py --help` | Shows all available commands. |

-----

## Generated Files

The parser outputs the following files:

| File | Description |
| :--- | :--- |
| `shop.csv` | Raw structural data of the research trees. |
| `localization.csv` | Vehicle names (Russian and English). |
| `wpcost.csv` | Economic data (RP cost, SL cost, BR). |
| `rank_requirements.csv` | Requirements to unlock subsequent ranks. |
| `country_flags.csv` | URLs for nation flag images. |
| `shop_images.csv` | URLs for vehicle images. |
| `version.csv` | The current datamine version. |
| `vehicles_merged.csv` | Final aggregated data for database import. |
| `dependencies.csv` | Vehicle research dependency graph. |