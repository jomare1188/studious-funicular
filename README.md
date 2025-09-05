Repository to cure metadata for citrus and sugarcane

## CITRUS - RNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `citrus`
    2. Filters: RNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Citrus"[Organism] OR citrus[All Fields]) AND ("biomol rna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

## CITRUS - DNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `citrus`
    2. Filters: DNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Citrus"[Organism] OR citrus[All Fields]) AND ("biomol dna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

## SACCHARUM - RNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `saccharum`
    2. Filters: RNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Saccharum"[Organism] OR Saccharum[All Fields]) AND ("biomol rna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

## SACCHARUM - DNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `saccharum`
    2. Filters: DNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Saccharum"[Organism] OR Saccharum[All Fields]) AND ("biomol dna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

---
### Usage
> [!CAUTION]
> Unstable version. Use with caution the scripts.

To run the script, we strongly recommend using **`uv`**.  
If you don’t have `uv` installed, you can find installation instructions here: [https://docs.astral.sh/uv/](https://docs.astral.sh/uv/)

1. `get_all_info.py`: used to retrieve information about SRA, particularly whether a PubMed ID is associated with the BioProject. This tool utilizes the NCBI Entrez API.

Parameters:  
- `email`: email to use the NCBI Entrez API
- `input`: the `.csv` file downloaded before in the NCBI _Run Selector_
- `output`: name of the `.csv` output.

```python
uv run get_all_info.py --email email@test.com --input your_csv_file.csv --output your_file_output.csv
```

2. `get_scholar.py`: used to retrieve information from Google Scholar for each BioProject, saving by default the first five results. Utilizes the Firefox WebDriver.

Parameters:  
- `input`: the `.csv` after the `get_all_info.py` execution
- `output`: name of the `.csv` output.

```python
uv run get_all_info.py --input your_csv_file.csv --output your_file_output.csv
```

3. `get_doi_from_url.py`: used to extract the DOI from the URL of each article.

Parameters:  
- `input`: the `.csv` after the `get_scholar.py` execution
- `json_dir`: the folder that contains the `json` files obtained from the execution of `get_scholar.py`.

```python
uv run get_doi_from_url.py --input your_csv_file.csv --json_dir json_directory
```

4. `load_dna.ipynb` and `load_rna.ipynb`: used for initial exploratory analysis and to extract basic information from the table obtained through NCBI _Run Selector_. You only need to update the `.csv` file path and execute each cell.
